//! Repository tooling, outside the product layer order.
//! Owns: resolved failure contracts, discarded outcomes and Option/Result panic APIs.
//! Depends on: compiler types, trait obligations and source-local boundary classifications.
//! Must not know: error-name conventions, product-file exemptions or runtime recovery policy.

use std::path::{Path, PathBuf};

use ahash::{HashSet, HashSetExt};
use rustc_hir::{Expr, ExprKind, HirId, PatKind, StmtKind, def::DefKind};
use rustc_infer::infer::TyCtxtInferExt;
use rustc_lint::LateContext;
use rustc_lint_defs::declare_tool_lint;
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::{
    Span, Symbol,
    def_id::{CRATE_DEF_INDEX, DefId, LocalDefId},
};
use rustc_trait_selection::traits::type_known_to_meet_bound_modulo_regions;

use crate::contracts::{Flow, INVALID_CONTRACT, Message};

declare_tool_lint! { pub nervix::BARE_ERROR_SIGNATURE, Deny, "Nervix failure contracts require contextual reports", report_in_external_macro: true }
declare_tool_lint! { pub nervix::DISCARDED_OUTCOME, Deny, "discarded failure outcomes require explicit classification", report_in_external_macro: true }
declare_tool_lint! { pub nervix::BARE_PANIC, Deny, "Option and Result panic operations require a stated guarantee", report_in_external_macro: true }

#[derive(Clone, Copy)]
enum Boundary {
    Outcome,
    Library,
}

struct PanicCall {
    definition: DefId,
    span: Span,
    token: Symbol,
}

fn boundary(tcx: TyCtxt<'_>, definition: DefId) -> Option<Boundary> {
    let path = [Symbol::intern("nervix"), Symbol::intern("error_boundary")];
    let attribute = tcx.get_attrs_by_path(definition, &path).next()?;
    parse_boundary(attribute)
}

fn return_boundary(tcx: TyCtxt<'_>, definition: DefId) -> Option<Boundary> {
    if let Some(contract) = boundary(tcx, definition) {
        return Some(contract);
    }
    let item = tcx.opt_associated_item(definition)?;
    boundary(tcx, item.trait_item_def_id()?)
}

fn parse_boundary(attribute: &rustc_hir::Attribute) -> Option<Boundary> {
    let arguments = attribute.meta_item_list()?;
    if arguments.len() != 2 {
        return None;
    }
    let kind = arguments[0].meta_item()?;
    if !kind.is_word() {
        return None;
    }
    let reason = arguments[1].meta_item()?;
    if !reason.has_name(Symbol::intern("reason"))
        || !reason
            .value_str()
            .is_some_and(|text| !text.as_str().trim().is_empty())
    {
        return None;
    }
    if kind.has_name(Symbol::intern("outcome")) {
        Some(Boundary::Outcome)
    } else if kind.has_name(Symbol::intern("library")) {
        Some(Boundary::Library)
    } else {
        None
    }
}

pub fn validate_boundary(cx: &LateContext<'_>, node: HirId, attribute: &rustc_hir::Attribute) {
    let placement = matches!(
        cx.tcx.hir_node(node),
        rustc_hir::Node::Item(rustc_hir::Item {
            kind: rustc_hir::ItemKind::Struct(..)
                | rustc_hir::ItemKind::Enum(..)
                | rustc_hir::ItemKind::Fn { .. },
            ..
        }) | rustc_hir::Node::ImplItem(rustc_hir::ImplItem {
            kind: rustc_hir::ImplItemKind::Fn(..),
            ..
        }) | rustc_hir::Node::TraitItem(rustc_hir::TraitItem {
            kind: rustc_hir::TraitItemKind::Fn(..),
            ..
        })
    );
    let path = [Symbol::intern("nervix"), Symbol::intern("error_boundary")];
    let count = cx
        .tcx
        .hir_attrs(node)
        .iter()
        .filter(|attr| attr.path_matches(&path))
        .count();
    if !placement || parse_boundary(attribute).is_none() || count != 1 {
        cx.tcx.emit_node_span_lint(
            INVALID_CONTRACT,
            node,
            attribute.span(),
            Message {
                message: "error_boundary requires outcome or library and a meaningful reason on \
                          its exact error type or return contract"
                    .into(),
            },
        );
    }
}

pub struct ErrorRules {
    root: PathBuf,
    signatures: HashSet<LocalDefId>,
}

impl ErrorRules {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            signatures: HashSet::new(),
        }
    }

    fn authored_type(&self, tcx: TyCtxt<'_>, definition: DefId) -> bool {
        let file = tcx
            .sess
            .source_map()
            .lookup_char_pos(tcx.def_span(definition).lo())
            .file
            .name
            .prefer_local_unconditionally()
            .to_string();
        let Ok(path) = Path::new(&file).canonicalize() else {
            return false;
        };
        let Ok(relative) = path.strip_prefix(&self.root) else {
            return false;
        };
        let path = relative.to_string_lossy();
        !path.starts_with("target/") && !path.contains("/target/")
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

    fn implements<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>, trait_id: DefId) -> bool {
        let (infcx, environment) = cx.tcx.infer_ctxt().build_with_typing_env(cx.typing_env());
        type_known_to_meet_bound_modulo_regions(&infcx, environment, value, trait_id)
    }

    fn shared_owner(&self, tcx: TyCtxt<'_>, definition: DefId) -> bool {
        matches!(
            tcx.crate_name(definition.krate).as_str(),
            "alloc" | "triomphe"
        ) && tcx.item_name(definition).as_str() == "Arc"
    }

    fn owned_failure<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>) -> Option<DefId> {
        let value = self.normalize(cx, value).peel_refs();
        let ty::Adt(adt, arguments) = value.kind() else {
            return None;
        };
        if self.authored_type(cx.tcx, adt.did()) {
            return Some(adt.did());
        }
        if cx.tcx.lang_items().owned_box() == Some(adt.did())
            || self.shared_owner(cx.tcx, adt.did())
        {
            return self.owned_failure(cx, arguments.type_at(0));
        }
        None
    }

    fn report_carrier<'tcx>(
        &self,
        cx: &LateContext<'tcx>,
        value: Ty<'tcx>,
        seen: &mut HashSet<Ty<'tcx>>,
    ) -> bool {
        let value = self.normalize(cx, value);
        if !seen.insert(value) {
            return false;
        }
        let ty::Adt(adt, arguments) = value.kind() else {
            return false;
        };
        if cx.tcx.crate_name(adt.did().krate).as_str() == "error_stack"
            && cx.tcx.item_name(adt.did()).as_str() == "Report"
        {
            return true;
        }
        let shared = self.shared_owner(cx.tcx, adt.did());
        if cx.tcx.lang_items().owned_box() == Some(adt.did()) || shared {
            return self.report_carrier(cx, arguments.type_at(0), seen);
        }
        // A carrier owns a complete report in every variant. Optional/collection fields do not
        // guarantee one. Box and shared ownership retain the report without changing that fact.
        for variant in adt.variants() {
            let mut holds_report = false;
            for field in &variant.fields {
                let mut path = seen.clone();
                if self.report_carrier(cx, field.ty(cx.tcx, arguments).skip_norm_wip(), &mut path) {
                    holds_report = true;
                    break;
                }
            }
            if !holds_report {
                return false;
            }
        }
        !adt.variants().is_empty()
    }

    fn stream_trait(&self, tcx: TyCtxt<'_>) -> Option<DefId> {
        for &krate in tcx.crates(()) {
            if tcx.crate_name(krate).as_str() != "futures_core" {
                continue;
            }
            let root = DefId {
                krate,
                index: CRATE_DEF_INDEX,
            };
            for child in tcx.module_children(root) {
                if child.ident.name.as_str() == "Stream" {
                    return child.res.opt_def_id();
                }
            }
        }
        None
    }

    fn bare_error<'tcx>(
        &self,
        cx: &LateContext<'tcx>,
        value: Ty<'tcx>,
        seen: &mut HashSet<Ty<'tcx>>,
    ) -> Option<Ty<'tcx>> {
        let value = self.normalize(cx, value);
        if !seen.insert(value) {
            return None;
        }
        if let ty::Adt(adt, arguments) = value.kind()
            && cx
                .tcx
                .is_diagnostic_item(Symbol::intern("Result"), adt.did())
        {
            let error = self.normalize(cx, arguments.type_at(1));
            if let Some(error_type) = self.owned_failure(cx, error)
                && boundary(cx.tcx, error_type).is_none()
                && let Some(error_trait) = cx.tcx.get_diagnostic_item(Symbol::intern("Error"))
                && self.implements(cx, error, error_trait)
                && !self.report_carrier(cx, error, &mut HashSet::new())
            {
                return Some(error);
            }
            return None;
        }
        // Resolve associated outputs, rather than walking all generic arguments: a Result in a
        // batch's success value is a separate per-row channel, not this function's failure.
        if let Some(future) = cx.tcx.lang_items().future_trait()
            && self.implements(cx, value, future)
            && let Some(output) = cx.get_associated_type(value, future, Symbol::intern("Output"))
            && let Some(error) = self.bare_error(cx, output, seen)
        {
            return Some(error);
        }
        if let Some(stream) = self.stream_trait(cx.tcx)
            && self.implements(cx, value, stream)
            && let Some(item) = cx.get_associated_type(value, stream, Symbol::intern("Item"))
        {
            return self.bare_error(cx, item, seen);
        }
        None
    }

    pub fn signature(
        &mut self,
        cx: &LateContext<'_>,
        owner: LocalDefId,
        span: Span,
        flow: &mut Flow,
    ) {
        if !self.signatures.insert(owner) {
            return;
        }
        let definition = owner.to_def_id();
        let output = match cx.tcx.def_kind(definition) {
            DefKind::Fn | DefKind::AssocFn => {
                let signature = cx
                    .tcx
                    .fn_sig(definition)
                    .instantiate_identity()
                    .skip_norm_wip();
                cx.tcx
                    .instantiate_bound_regions_with_erased(signature)
                    .output()
            }
            DefKind::Closure => {
                let value = cx
                    .tcx
                    .type_of(definition)
                    .instantiate_identity()
                    .skip_norm_wip();
                match value.kind() {
                    ty::Closure(_, arguments) => cx
                        .tcx
                        .instantiate_bound_regions_with_erased(arguments.as_closure().sig())
                        .output(),
                    ty::Coroutine(_, arguments) => arguments.as_coroutine().return_ty(),
                    ty::CoroutineClosure(_, arguments) => {
                        cx.tcx
                            .instantiate_bound_regions_with_erased(
                                arguments.as_coroutine_closure().coroutine_closure_sig(),
                            )
                            .return_ty
                    }
                    _ => return,
                }
            }
            _ => return,
        };
        let contract = return_boundary(cx.tcx, definition);
        if contract.is_some() {
            return;
        }
        if let Some(error) = self.bare_error(cx, output, &mut HashSet::new()) {
            flow.diagnostic(
                cx,
                cx.tcx.local_def_id_to_hir_id(owner),
                span,
                BARE_ERROR_SIGNATURE,
                format!(
                    "return contract carries Nervix failure {error} without its contextual \
                     Report; return error_stack::Result or a carrier owning the whole report"
                ),
            );
        }
    }

    fn loses_outcome<'tcx>(
        &self,
        cx: &LateContext<'tcx>,
        value: Ty<'tcx>,
        seen: &mut HashSet<Ty<'tcx>>,
    ) -> bool {
        let value = self.normalize(cx, value);
        if !seen.insert(value) {
            return false;
        }
        match value.kind() {
            ty::Adt(adt, arguments) => {
                if cx
                    .tcx
                    .is_diagnostic_item(Symbol::intern("Result"), adt.did())
                {
                    return true;
                }
                if cx.tcx.crate_name(adt.did().krate).as_str() == "error_stack"
                    && cx.tcx.item_name(adt.did()).as_str() == "Report"
                {
                    return true;
                }
                if cx.tcx.lang_items().owned_box() == Some(adt.did())
                    || self.shared_owner(cx.tcx, adt.did())
                {
                    return self.loses_outcome(cx, arguments.type_at(0), seen);
                }
                // These containers own their elements even though their compiler-visible fields
                // store them through raw pointers. PhantomData and borrowed pointers own none.
                let crate_name = cx.tcx.crate_name(adt.did().krate);
                let name = cx.tcx.item_name(adt.did());
                let elements = match (crate_name.as_str(), name.as_str()) {
                    ("alloc", "Vec" | "VecDeque" | "LinkedList" | "BinaryHeap" | "BTreeSet")
                    | ("std", "HashSet") => 1,
                    ("alloc", "BTreeMap") | ("std", "HashMap") => 2,
                    _ => 0,
                };
                for index in 0..elements {
                    if self.loses_outcome(cx, arguments.type_at(index), seen) {
                        return true;
                    }
                }
                if elements != 0 {
                    return false;
                }
                if let Some(error_trait) = cx.tcx.get_diagnostic_item(Symbol::intern("Error"))
                    && self.implements(cx, value, error_trait)
                    && self.report_carrier(cx, value, &mut HashSet::new())
                {
                    return true;
                }
                let optional = cx
                    .tcx
                    .is_diagnostic_item(Symbol::intern("Option"), adt.did());
                if !optional && (!self.authored_type(cx.tcx, adt.did()) || adt.has_dtor(cx.tcx)) {
                    return false;
                }
                // An outcome wrapper transparently owns one value. Multi-field resource state
                // and guards have their own lifetime; their retained reports are observations.
                for variant in adt.variants() {
                    if variant.fields.len() != 1 {
                        continue;
                    }
                    for field in &variant.fields {
                        if self.loses_outcome(cx, field.ty(cx.tcx, arguments).skip_norm_wip(), seen)
                        {
                            return true;
                        }
                    }
                }
                false
            }
            ty::Tuple(fields) => fields
                .iter()
                .any(|field| self.loses_outcome(cx, field, seen)),
            ty::Array(element, _) => self.loses_outcome(cx, *element, seen),
            _ => false,
        }
    }

    fn classification_owner(&self, cx: &LateContext<'_>, node: HirId) -> bool {
        let owner = cx.tcx.hir_enclosing_body_owner(node).to_def_id();
        let Some(item) = cx.tcx.opt_associated_item(owner) else {
            return false;
        };
        let Some(trait_item) = item.trait_item_def_id() else {
            return false;
        };
        let Some(trait_id) = cx.tcx.trait_of_assoc(trait_item) else {
            return false;
        };
        cx.tcx.crate_name(trait_id.krate).as_str() == "nervix_recovery"
            && matches!(
                cx.tcx.item_name(trait_id).as_str(),
                "Discarded" | "Reported" | "NoReceiver"
            )
    }

    fn discard(
        &self,
        cx: &LateContext<'_>,
        expression: &Expr<'_>,
        node: HirId,
        span: Span,
        flow: &mut Flow,
    ) {
        if self.classification_owner(cx, node) {
            return;
        }
        if self.loses_outcome(
            cx,
            cx.typeck_results().expr_ty(expression),
            &mut HashSet::new(),
        ) {
            flow.diagnostic(
                cx,
                node,
                span,
                DISCARDED_OUTCOME,
                "this operation drops a failure outcome; propagate it, match its outcome, or use \
                 the resolved meticulous/nervix-recovery classification with its reason"
                    .into(),
            );
        }
    }

    pub fn binding(&self, cx: &LateContext<'_>, local: &rustc_hir::LetStmt<'_>, flow: &mut Flow) {
        if local.init.is_some() {
            self.pattern(cx, local.pat, local.hir_id, flow);
        }
    }

    fn pattern(
        &self,
        cx: &LateContext<'_>,
        pattern: &rustc_hir::Pat<'_>,
        node: HirId,
        flow: &mut Flow,
    ) {
        match pattern.kind {
            PatKind::Wild => {
                if !self.classification_owner(cx, node)
                    && self.loses_outcome(
                        cx,
                        cx.typeck_results().pat_ty(pattern),
                        &mut HashSet::new(),
                    )
                {
                    flow.diagnostic(
                        cx,
                        node,
                        pattern.span,
                        DISCARDED_OUTCOME,
                        "this wildcard drops a failure outcome; match its outcome or classify the \
                         discard with its reason"
                            .into(),
                    );
                }
            }
            PatKind::Tuple(patterns, _) => {
                for pattern in patterns {
                    self.pattern(cx, pattern, node, flow);
                }
            }
            PatKind::Struct(_, fields, _) => {
                let value = self.normalize(cx, cx.typeck_results().pat_ty(pattern));
                if let ty::Adt(adt, _) = value.kind()
                    && !adt.is_enum()
                {
                    for field in fields {
                        self.pattern(cx, field.pat, node, flow);
                    }
                }
            }
            PatKind::TupleStruct(_, patterns, _) => {
                let value = self.normalize(cx, cx.typeck_results().pat_ty(pattern));
                if let ty::Adt(adt, _) = value.kind()
                    && !adt.is_enum()
                {
                    for pattern in patterns {
                        self.pattern(cx, pattern, node, flow);
                    }
                }
            }
            _ => {}
        }
    }

    fn instantiated_contract<'tcx>(
        &self,
        cx: &LateContext<'tcx>,
        expression: &Expr<'_>,
        definition: DefId,
        arguments: ty::GenericArgsRef<'tcx>,
        flow: &mut Flow,
    ) {
        if !self.authored_type(cx.tcx, definition) || boundary(cx.tcx, definition).is_some() {
            return;
        }
        if let Some(item) = cx.tcx.opt_associated_item(definition)
            && let Some(trait_item) = item.trait_item_def_id()
            && boundary(cx.tcx, trait_item).is_some()
        {
            return;
        }
        let signature = cx
            .tcx
            .fn_sig(definition)
            .instantiate(cx.tcx, arguments)
            .skip_norm_wip();
        let output = cx
            .tcx
            .instantiate_bound_regions_with_erased(signature)
            .output();
        let generic = cx
            .tcx
            .fn_sig(definition)
            .instantiate_identity()
            .skip_norm_wip();
        let declared = cx
            .tcx
            .instantiate_bound_regions_with_erased(generic)
            .output();
        if output == declared {
            return;
        }
        if let Some(error) = self.bare_error(cx, output, &mut HashSet::new()) {
            flow.diagnostic(
                cx,
                expression.hir_id,
                expression.span,
                BARE_ERROR_SIGNATURE,
                format!(
                    "this instantiated return contract carries Nervix failure {error} without its \
                     contextual Report"
                ),
            );
        }
    }

    pub fn statement(
        &self,
        cx: &LateContext<'_>,
        statement: &rustc_hir::Stmt<'_>,
        flow: &mut Flow,
    ) {
        if let StmtKind::Semi(expression) = statement.kind {
            self.discard(cx, expression, statement.hir_id, expression.span, flow);
        }
    }

    pub fn expression(&self, cx: &LateContext<'_>, expression: &Expr<'_>, flow: &mut Flow) {
        let typeck = cx.typeck_results();
        match expression.kind {
            ExprKind::Call(function, _) => {
                if let ty::FnDef(definition, arguments) = *typeck.expr_ty(function).kind() {
                    self.instantiated_contract(
                        cx,
                        expression,
                        definition,
                        cx.tcx.instantiate_bound_regions_with_erased(arguments),
                        flow,
                    );
                }
            }
            ExprKind::MethodCall(..) => {
                if let Some(definition) = typeck.type_dependent_def_id(expression.hir_id) {
                    self.instantiated_contract(
                        cx,
                        expression,
                        definition,
                        typeck.node_args(expression.hir_id),
                        flow,
                    );
                }
            }
            _ => {}
        }
        let call = match expression.kind {
            ExprKind::MethodCall(segment, ..) => {
                let Some(definition) = typeck.type_dependent_def_id(expression.hir_id) else {
                    return;
                };
                PanicCall {
                    definition,
                    span: segment.ident.span,
                    token: segment.ident.name,
                }
            }
            ExprKind::Call(function, arguments) => {
                let ty::FnDef(definition, _) = *typeck.expr_ty(function).kind() else {
                    return;
                };
                if cx
                    .tcx
                    .is_diagnostic_item(Symbol::intern("mem_drop"), definition)
                    && let Some(value) = arguments.first()
                {
                    self.discard(cx, value, expression.hir_id, expression.span, flow);
                    return;
                }
                let segment = match function.kind {
                    ExprKind::Path(rustc_hir::QPath::Resolved(_, path)) => path.segments.last(),
                    ExprKind::Path(rustc_hir::QPath::TypeRelative(_, segment)) => Some(segment),
                    _ => None,
                };
                let Some(segment) = segment else {
                    return;
                };
                PanicCall {
                    definition,
                    span: segment.ident.span,
                    token: segment.ident.name,
                }
            }
            _ => return,
        };
        if !matches!(
            cx.tcx.item_name(call.definition).as_str(),
            "unwrap" | "expect"
        ) {
            return;
        }
        // Proc macros can attach an authored derive/function span to generated code. Only an
        // actual authored callee token is governed; tokens supplied to a macro keep their span.
        let Ok(source) = cx.tcx.sess.source_map().span_to_snippet(call.span) else {
            return;
        };
        let authored_callee = source.trim() == call.token.as_str();
        if !authored_callee {
            return;
        }
        let Some(implementation) = cx.tcx.inherent_impl_of_assoc(call.definition) else {
            return;
        };
        let owner = self.normalize(
            cx,
            cx.tcx
                .type_of(implementation)
                .instantiate_identity()
                .skip_norm_wip(),
        );
        let ty::Adt(adt, _) = owner.kind() else {
            return;
        };
        if cx
            .tcx
            .is_diagnostic_item(Symbol::intern("Option"), adt.did())
            || cx
                .tcx
                .is_diagnostic_item(Symbol::intern("Result"), adt.did())
        {
            flow.diagnostic(
                cx,
                expression.hir_id,
                call.span,
                BARE_PANIC,
                "the resolved Option/Result panic API needs a stated construction or checked \
                 guarantee; use meticulous::assured/verified, or return a typed failure"
                    .into(),
            );
        }
    }
}
