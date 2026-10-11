//! Repository tooling, outside the product layer order.
//! Owns: source contract validation, inheritance and supported compiler-visible call effects.
//! Depends on: HIR, definition metadata and generated report vocabulary.
//! Must not know: file-based classifications or whole-program runtime ownership.

use std::collections::BTreeMap;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use nervix_lint_report::{Context, ContractProblem, ReportError};
use rustc_errors::{Diag, Diagnostic};
use rustc_hir::{Expr, HirId, def::DefKind};
use rustc_lint::LateContext;
use rustc_lint_defs::{Lint, StableLintExpectationId, declare_tool_lint};
use rustc_middle::ty::{self, TyCtxt};
use rustc_span::{
    Span, Symbol,
    def_id::{DefId, LocalDefId},
};

declare_tool_lint! { pub nervix::SYNC_ACQUISITION, Deny, "recurring synchronization requires a bounded source contract", report_in_external_macro: true }
declare_tool_lint! { pub nervix::LIFECYCLE_CALL, Deny, "recurring execution cannot enter a lifecycle-only contract", report_in_external_macro: true }
declare_tool_lint! { pub nervix::UNKNOWN_EFFECT, Deny, "unknown synchronization or callback effects require a source contract", report_in_external_macro: true }
declare_tool_lint! { pub nervix::INVALID_CONTRACT, Deny, "source contracts and narrow expectations must be valid", report_in_external_macro: true }

pub struct Message {
    pub message: String,
}

impl<'a> Diagnostic<'a, ()> for Message {
    fn into_diag(
        self,
        dcx: rustc_errors::DiagCtxtHandle<'a>,
        level: rustc_errors::Level,
    ) -> Diag<'a, ()> {
        Diag::new(dcx, level, self.message)
    }
}

#[derive(Default)]
pub struct Flow {
    nodes: HashSet<LocalDefId>,
    edges: HashMap<LocalDefId, HashSet<LocalDefId>>,
    calls: Vec<Call>,
    acquisitions: Vec<Operation>,
    validated: HashSet<HirId>,
    diagnostics: Vec<(HirId, &'static Lint)>,
}

struct Call {
    owner: LocalDefId,
    target: Option<DefId>,
    node: HirId,
    span: Span,
    indirect: bool,
}

struct Operation {
    owner: LocalDefId,
    node: HirId,
    span: Span,
    finding: usize,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ExpectationKey {
    attribute: rustc_ast::AttrId,
    lint_index: u16,
}

#[derive(Eq, Hash, PartialEq)]
struct ExpectedLint {
    expectation: ExpectationKey,
    lint: String,
}

struct ReviewedOperations {
    expectation: StableLintExpectationId,
    operations: HashSet<HirId>,
}

type ExpectationCounts = HashMap<ExpectedLint, ReviewedOperations>;

/// Narrowest metadata contract wins. Parent contracts supply defaults for their definitions;
/// call reachability can make a helper recurring even when its inherited default is cold.
pub fn context(tcx: TyCtxt<'_>, mut definition: DefId) -> Option<Context> {
    loop {
        let path = [Symbol::intern("nervix"), Symbol::intern("context")];
        if let Some(attribute) = tcx.get_attrs_by_path(definition, &path).next() {
            return parse_context(attribute).ok();
        }
        if let Some(value) = binding_context(tcx, definition) {
            return Some(value);
        }
        // An implementation inherits a trait's method contract before its module default.
        if let Some(item) = tcx.opt_associated_item(definition)
            && let Some(trait_item) = item.trait_item_def_id()
            && let Some(value) = context(tcx, trait_item)
        {
            return Some(value);
        }
        if matches!(tcx.def_kind(definition), DefKind::Impl { .. })
            && let ty::Adt(adt, _) = tcx
                .type_of(definition)
                .instantiate_identity()
                .skip_norm_wip()
                .kind()
        {
            let path = [Symbol::intern("nervix"), Symbol::intern("context")];
            if let Some(attribute) = tcx.get_attrs_by_path(adt.did(), &path).next() {
                return parse_context(attribute).ok();
            }
        }
        // Derives implement data operations, not the module's task entry contract. An explicit
        // type or method contract above still applies; acquisitions remain independently checked.
        if matches!(tcx.def_kind(definition), DefKind::Impl { .. })
            && tcx.is_automatically_derived(definition)
        {
            return None;
        }
        definition = tcx.opt_parent(definition)?;
    }
}

/// Module/crate frequency defaults seed analysis; a supported local caller can refine them.
/// A callable, trait, impl or type's lifecycle contract is an installation boundary instead.
fn lifecycle_boundary(tcx: TyCtxt<'_>, mut definition: DefId) -> bool {
    if !definition.is_local() {
        return context(tcx, definition).is_some_and(|value| value.is_lifecycle());
    }
    loop {
        if matches!(tcx.def_kind(definition), DefKind::Mod) {
            return false;
        }
        let path = [Symbol::intern("nervix"), Symbol::intern("context")];
        if let Some(attribute) = tcx.get_attrs_by_path(definition, &path).next() {
            return parse_context(attribute).is_ok_and(|value| value.is_lifecycle());
        }
        if let Some(value) = binding_context(tcx, definition) {
            return value.is_lifecycle();
        }
        if let Some(item) = tcx.opt_associated_item(definition)
            && let Some(trait_item) = item.trait_item_def_id()
            && lifecycle_boundary(tcx, trait_item)
        {
            return true;
        }
        if matches!(tcx.def_kind(definition), DefKind::Impl { .. })
            && let ty::Adt(adt, _) = tcx
                .type_of(definition)
                .instantiate_identity()
                .skip_norm_wip()
                .kind()
            && let Some(attribute) = tcx.get_attrs_by_path(adt.did(), &path).next()
        {
            return parse_context(attribute).is_ok_and(|value| value.is_lifecycle());
        }
        let Some(parent) = tcx.opt_parent(definition) else {
            return false;
        };
        definition = parent;
    }
}

/// Dynamic or generic dispatch needs a callable/trait/type contract, not an inherited cold
/// module default. Local concrete bodies are followed independently by the call graph.
pub fn has_callee_contract(tcx: TyCtxt<'_>, mut definition: DefId) -> bool {
    loop {
        if matches!(tcx.def_kind(definition), DefKind::Mod) {
            return false;
        }
        if tcx
            .get_attrs_by_path(
                definition,
                &[Symbol::intern("nervix"), Symbol::intern("context")],
            )
            .next()
            .is_some()
        {
            return true;
        }
        if let Some(item) = tcx.opt_associated_item(definition)
            && let Some(trait_item) = item.trait_item_def_id()
            && has_callee_contract(tcx, trait_item)
        {
            return true;
        }
        let Some(parent) = tcx.opt_parent(definition) else {
            return false;
        };
        definition = parent;
    }
}

fn parse_context(
    attribute: &rustc_hir::Attribute,
) -> Result<Context, error_stack::Report<ReportError>> {
    let invalid = |problem| error_stack::Report::new(ReportError::InvalidContract { problem });
    let Some(arguments) = attribute.meta_item_list() else {
        return Err(invalid(ContractProblem::MissingArguments));
    };
    let mut kind = None;
    let mut values = BTreeMap::new();
    for argument in &arguments {
        let Some(item) = argument.meta_item() else {
            return Err(invalid(ContractProblem::UnnamedArgument));
        };
        let Some(name) = item.ident() else {
            return Err(invalid(ContractProblem::QualifiedArgument));
        };
        if item.is_word() {
            if kind.replace(name.name.to_string()).is_some() {
                return Err(invalid(ContractProblem::MultipleKinds));
            }
        } else {
            let Some(value) = item.value_str() else {
                return Err(invalid(ContractProblem::NonStringValue {
                    argument: name.name.to_string(),
                }));
            };
            if values
                .insert(name.name.to_string(), value.to_string())
                .is_some()
            {
                return Err(invalid(ContractProblem::DuplicateArgument {
                    argument: name.name.to_string(),
                }));
            }
        }
    }
    let Some(kind) = kind else {
        return Err(invalid(ContractProblem::MissingKind));
    };
    let Some(reason) = values.remove("reason") else {
        return Err(invalid(ContractProblem::MissingReason));
    };
    let key = values.remove("key");
    let bound = values.remove("bound");
    if let Some((argument, _)) = values.first_key_value() {
        return Err(invalid(ContractProblem::UnknownArgument {
            argument: argument.clone(),
        }));
    }
    Context::from_parts(&kind, reason, key, bound)
}

impl Flow {
    pub fn diagnostic(
        &mut self,
        cx: &LateContext<'_>,
        node: HirId,
        span: Span,
        lint: &'static Lint,
        message: String,
    ) {
        self.diagnostics.push((node, lint));
        cx.tcx
            .emit_node_span_lint(lint, node, span, Message { message });
    }

    pub fn validate(&mut self, cx: &LateContext<'_>, node: HirId, placement: bool) {
        if !self.validated.insert(node) {
            return;
        }
        let mut contexts = 0;
        let mut dispatches = 0;
        for attribute in cx.tcx.hir_attrs(node) {
            let path = attribute.path();
            if path.first().is_some_and(|name| name.as_str() == "nervix")
                && (path.len() != 2
                    || !matches!(path[1].as_str(), "context" | "dispatch" | "error_boundary"))
            {
                cx.tcx.emit_node_span_lint(
                    INVALID_CONTRACT,
                    node,
                    attribute.span(),
                    Message {
                        message: "unknown Nervix source annotation; use context, dispatch or \
                                  error_boundary"
                            .into(),
                    },
                );
            }
            if attribute.path_matches(&[Symbol::intern("nervix"), Symbol::intern("error_boundary")])
            {
                crate::errors::validate_boundary(cx, node, attribute);
            }
            if attribute.path_matches(&[Symbol::intern("nervix"), Symbol::intern("context")]) {
                contexts += 1;
                let anonymous_body = binding(cx.tcx.hir_node(node))
                    .is_some_and(|local| local.init.is_some_and(owns_one_body));
                let problem = if !placement && !anonymous_body {
                    Some(
                        "context belongs on a function, closure, trait, type, impl, module, crate \
                         or binding owning one anonymous body"
                            .into(),
                    )
                } else {
                    parse_context(attribute)
                        .err()
                        .map(|error| error.current_context().to_string())
                };
                if let Some(message) = problem {
                    cx.tcx.emit_node_span_lint(
                        INVALID_CONTRACT,
                        node,
                        attribute.span(),
                        Message { message },
                    );
                }
                if let rustc_hir::Node::ImplItem(implementation) = cx.tcx.hir_node(node)
                    && let Some(item) = cx
                        .tcx
                        .opt_associated_item(implementation.owner_id.to_def_id())
                    && let Some(trait_item) = item.trait_item_def_id()
                    && parse_context(attribute).is_ok_and(|value| !value.is_recurring())
                    && context(cx.tcx, trait_item).is_some_and(|value| value.is_recurring())
                {
                    cx.tcx.emit_node_span_lint(
                        INVALID_CONTRACT,
                        node,
                        attribute.span(),
                        Message {
                            message: "an implementation cannot weaken its recurring trait method \
                                      to a non-recurring context"
                                .into(),
                        },
                    );
                }
            }
            if attribute.path_matches(&[Symbol::intern("nervix"), Symbol::intern("dispatch")]) {
                dispatches += 1;
                let arguments = attribute.meta_item_list();
                let valid = arguments.as_ref().is_some_and(|items| {
                    items.len() == 1
                        && items[0].meta_item().is_some_and(|item| {
                            item.has_name(Symbol::intern("reason"))
                                && item
                                    .value_str()
                                    .is_some_and(|reason| !reason.as_str().trim().is_empty())
                        })
                });
                let callable = matches!(
                    cx.tcx.hir_node(node),
                    rustc_hir::Node::Expr(rustc_hir::Expr {
                        kind: rustc_hir::ExprKind::Closure(..),
                        ..
                    }) | rustc_hir::Node::Item(rustc_hir::Item {
                        kind: rustc_hir::ItemKind::Fn { .. },
                        ..
                    }) | rustc_hir::Node::ImplItem(rustc_hir::ImplItem {
                        kind: rustc_hir::ImplItemKind::Fn(..),
                        ..
                    }) | rustc_hir::Node::TraitItem(rustc_hir::TraitItem {
                        kind: rustc_hir::TraitItemKind::Fn(..),
                        ..
                    })
                );
                if !callable || !valid {
                    cx.tcx.emit_node_span_lint(
                        INVALID_CONTRACT,
                        node,
                        attribute.span(),
                        Message {
                            message: "dispatch requires a reason on its owning function or closure"
                                .into(),
                        },
                    );
                }
            }
            let level = attribute.name();
            if let Some(level) = level
                && matches!(level.as_str(), "allow" | "expect")
                && let Some(arguments) = attribute.meta_item_list()
            {
                let warnings = arguments.iter().any(|item| {
                    item.meta_item()
                        .is_some_and(|item| item.has_name(Symbol::intern("warnings")))
                });
                let governed = warnings
                    || arguments.iter().any(|item| {
                        item.meta_item().is_some_and(|item| {
                            item.path
                                .segments
                                .first()
                                .is_some_and(|segment| segment.ident.name.as_str() == "nervix")
                        })
                    });
                if !governed {
                    continue;
                }
                let reason = arguments.iter().any(|item| {
                    item.meta_item().is_some_and(|item| {
                        item.has_name(Symbol::intern("reason"))
                            && item
                                .value_str()
                                .is_some_and(|value| !value.as_str().trim().is_empty())
                    })
                });
                // A normal expect records at least one occurrence, not its exact cardinality.
                // Restrict it to an operation expression or its binding so unrelated new
                // operations cannot inherit a function/module-wide suppression.
                let narrow = matches!(
                    cx.tcx.hir_node(node),
                    rustc_hir::Node::Expr(_)
                        | rustc_hir::Node::LetStmt(_)
                        | rustc_hir::Node::Stmt(_)
                );
                if level.as_str() == "allow" || warnings || !reason || !narrow {
                    cx.tcx.emit_node_span_lint(
                        INVALID_CONTRACT,
                        node,
                        attribute.span(),
                        Message {
                            message: "Nervix exceptions require a reason-bearing expect on one \
                                      operation or binding; blanket suppression is forbidden"
                                .into(),
                        },
                    );
                }
            }
        }
        if contexts > 1 {
            cx.tcx.emit_node_span_lint(
                INVALID_CONTRACT,
                node,
                cx.tcx.hir_span(node),
                Message {
                    message: "conflicting context contracts on the same owner".into(),
                },
            );
        }
        if dispatches > 1 {
            cx.tcx.emit_node_span_lint(
                INVALID_CONTRACT,
                node,
                cx.tcx.hir_span(node),
                Message {
                    message: "conflicting dispatch contracts on the same owner".into(),
                },
            );
        }
    }

    pub fn body(&mut self, cx: &LateContext<'_>, body: &rustc_hir::Body<'_>) {
        let owner = cx.tcx.hir_body_owner_def_id(body.id());
        for parameter in body.params {
            self.validate(cx, parameter.hir_id, false);
        }
        self.nodes.insert(owner);
        let kind = cx.tcx.def_kind(owner);
        if matches!(kind, DefKind::Closure | DefKind::SyntheticCoroutineBody)
            && let Some(parent) = cx
                .tcx
                .opt_parent(owner.to_def_id())
                .and_then(DefId::as_local)
        {
            self.edges.entry(parent).or_default().insert(owner);
        }
    }

    pub fn call(
        &mut self,
        cx: &LateContext<'_>,
        expression: &Expr<'_>,
        target: Option<DefId>,
        indirect: bool,
        authored: bool,
    ) {
        if target.is_some_and(|target| {
            !matches!(
                cx.tcx.def_kind(target),
                DefKind::Fn | DefKind::AssocFn | DefKind::Closure
            )
        }) {
            return;
        }
        let owner = cx.tcx.hir_enclosing_body_owner(expression.hir_id);
        self.nodes.insert(owner);
        if let Some(local) = target.and_then(DefId::as_local) {
            self.edges.entry(owner).or_default().insert(local);
        }
        if !authored || expression.span.desugaring_kind() == Some(rustc_span::DesugaringKind::Await)
        {
            return;
        }
        self.calls.push(Call {
            owner,
            target,
            node: expression.hir_id,
            span: expression.span,
            indirect,
        });
    }

    pub fn acquisition(
        &mut self,
        cx: &LateContext<'_>,
        expression: &Expr<'_>,
        span: Span,
        finding: usize,
    ) {
        let owner = cx.tcx.hir_enclosing_body_owner(expression.hir_id);
        self.nodes.insert(owner);
        self.acquisitions.push(Operation {
            owner,
            node: expression.hir_id,
            span,
            finding,
        });
    }

    pub fn finish(&self, cx: &LateContext<'_>, findings: &mut [nervix_lint_report::Finding]) {
        let mut recurring = HashSet::new();
        for owner in &self.nodes {
            if context(cx.tcx, owner.to_def_id()).is_some_and(|value| value.is_recurring()) {
                recurring.insert(*owner);
            }
        }
        // Finite local call graph, revisited for this compilation. Unannotated helpers and
        // closure bodies inherit the hottest supported caller rather than a stored review.
        loop {
            let mut changed = false;
            for (caller, callees) in &self.edges {
                if recurring.contains(caller) {
                    for callee in callees {
                        // A lifecycle contract is an explicit call boundary. Diagnose its caller
                        // below; do not also reclassify the installation's internal operations.
                        if !lifecycle_boundary(cx.tcx, callee.to_def_id()) {
                            changed |= recurring.insert(*callee);
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        let mut expectations = HashMap::new();
        for &(node, lint) in &self.diagnostics {
            self.count_expectation(cx, &mut expectations, node, lint);
        }
        for operation in &self.acquisitions {
            let declared = context(cx.tcx, operation.owner.to_def_id());
            let actual = if recurring.contains(&operation.owner)
                && !declared.as_ref().is_some_and(Context::is_recurring)
            {
                Some(Context::Recurring {
                    reason: "reachable from a recurring source contract".into(),
                })
            } else {
                declared
            };
            let finding = &mut findings[operation.finding];
            finding.context = actual.clone();
            match actual {
                Some(value) if !value.permits_acquisition() => {
                    self.count_expectation(cx, &mut expectations, operation.node, SYNC_ACQUISITION);
                    cx.tcx.emit_node_span_lint(
                        SYNC_ACQUISITION,
                        operation.node,
                        operation.span,
                        Message {
                            message: format!(
                                "recurring {}::{} acquires synchronization; retain the resolved \
                                 handle or document its bounded protocol",
                                finding.receiver, finding.operation
                            ),
                        },
                    );
                }
                None => {
                    self.count_expectation(cx, &mut expectations, operation.node, UNKNOWN_EFFECT);
                    cx.tcx.emit_node_span_lint(
                        UNKNOWN_EFFECT,
                        operation.node,
                        operation.span,
                        Message {
                            message: "synchronization owner has no execution contract; declare \
                                      its actual caller frequency"
                                .into(),
                        },
                    );
                }
                _ => {}
            }
        }
        for call in &self.calls {
            if !recurring.contains(&call.owner) {
                continue;
            }
            if let Some(target) = call.target
                && lifecycle_boundary(cx.tcx, target)
            {
                self.count_expectation(cx, &mut expectations, call.node, LIFECYCLE_CALL);
                cx.tcx.emit_node_span_lint(
                    LIFECYCLE_CALL,
                    call.node,
                    call.span,
                    Message {
                        message: format!(
                            "recurring caller enters lifecycle-only {}; retain its installed \
                             result before recurring execution",
                            cx.tcx.def_path_str(target)
                        ),
                    },
                );
            }
            if call.indirect && !self.has_dispatch(cx.tcx, call.owner) {
                self.count_expectation(cx, &mut expectations, call.node, UNKNOWN_EFFECT);
                cx.tcx.emit_node_span_lint(
                    UNKNOWN_EFFECT,
                    call.node,
                    call.span,
                    Message {
                        message: format!(
                            "indirect dispatch in {} has unknown effects; put a context on its \
                             trait/callee or a reason-bearing dispatch contract on its owner",
                            cx.tcx.def_path_str(call.owner)
                        ),
                    },
                );
            }
        }
        let fulfilled: HashSet<_> = expectations
            .keys()
            .map(|expected| expected.expectation)
            .collect();
        for (expectation, contract) in cx.tcx.lint_expectations(()) {
            if contract.lint_tool == Some(Symbol::intern("nervix"))
                && !fulfilled.contains(&expectation_key(cx.tcx, *expectation))
            {
                // Rust's builtin unfulfilled lint omits external macro expansions. Nervix
                // operation macros carry the same obligation, including when their body changes.
                cx.tcx.emit_node_span_lint(
                    INVALID_CONTRACT,
                    expectation.hir_id,
                    contract.emission_span,
                    Message {
                        message: "Nervix expectation is unfulfilled; remove the expectation or \
                                  restore its reviewed operation"
                            .into(),
                    },
                );
            }
        }
        for (expected, reviewed) in expectations {
            if reviewed.operations.len() > 1 {
                cx.tcx.emit_node_span_lint(
                    INVALID_CONTRACT,
                    reviewed.expectation.hir_id,
                    cx.tcx.hir_span(reviewed.expectation.hir_id),
                    Message {
                        message: format!(
                            "expect({}) covers {} distinct operations; attach an expectation to \
                             each reviewed operation",
                            expected.lint,
                            reviewed.operations.len()
                        ),
                    },
                );
            }
        }
    }

    fn count_expectation(
        &self,
        cx: &LateContext<'_>,
        counts: &mut ExpectationCounts,
        node: HirId,
        lint: &'static Lint,
    ) {
        // Ask Rust's effective lint level: macro expansion and HIR lowering can place the
        // expectation outside the syntactic ancestor list. Its compiler ID is authoritative.
        if let Some(expectation) = cx.tcx.lint_level_spec_at_node(lint, node).lint_id() {
            counts
                .entry(ExpectedLint {
                    expectation: expectation_key(cx.tcx, expectation),
                    lint: lint.name_lower(),
                })
                .or_insert_with(|| ReviewedOperations {
                    expectation,
                    operations: HashSet::new(),
                })
                .operations
                .insert(node);
        }
    }

    fn has_dispatch(&self, tcx: TyCtxt<'_>, owner: LocalDefId) -> bool {
        let mut definition = owner.to_def_id();
        loop {
            if tcx
                .get_attrs_by_path(
                    definition,
                    &[Symbol::intern("nervix"), Symbol::intern("dispatch")],
                )
                .next()
                .is_some()
            {
                return true;
            }
            if let Some(item) = tcx.opt_associated_item(definition)
                && let Some(trait_item) = item.trait_item_def_id()
                && tcx
                    .get_attrs_by_path(
                        trait_item,
                        &[Symbol::intern("nervix"), Symbol::intern("dispatch")],
                    )
                    .next()
                    .is_some()
            {
                return true;
            }
            if !matches!(
                tcx.def_kind(definition),
                DefKind::Closure | DefKind::SyntheticCoroutineBody
            ) {
                return false;
            }
            let Some(parent) = tcx.opt_parent(definition) else {
                return false;
            };
            definition = parent;
        }
    }
}

fn expectation_key(tcx: TyCtxt<'_>, expectation: StableLintExpectationId) -> ExpectationKey {
    // Like rustc's expectation check, canonicalize copied HIR attributes from one expansion.
    ExpectationKey {
        attribute: tcx.hir_attrs(expectation.hir_id)[usize::from(expectation.attr_index)].id(),
        lint_index: expectation.lint_index,
    }
}

fn binding_context(tcx: TyCtxt<'_>, definition: DefId) -> Option<Context> {
    if !matches!(
        tcx.def_kind(definition),
        DefKind::Closure | DefKind::SyntheticCoroutineBody
    ) {
        return None;
    }
    let local = definition.as_local()?;
    let path = [Symbol::intern("nervix"), Symbol::intern("context")];
    for (parent, node) in tcx.hir_parent_iter(tcx.local_def_id_to_hir_id(local)) {
        if binding(node).is_some() {
            return tcx
                .hir_attrs(parent)
                .iter()
                .find(|attribute| attribute.path_matches(&path))
                .and_then(|attribute| parse_context(attribute).ok());
        }
        if matches!(
            node,
            rustc_hir::Node::Item(_) | rustc_hir::Node::ImplItem(_) | rustc_hir::Node::TraitItem(_)
        ) {
            break;
        }
    }
    None
}

fn binding(node: rustc_hir::Node<'_>) -> Option<&rustc_hir::LetStmt<'_>> {
    match node {
        rustc_hir::Node::LetStmt(local) => Some(local),
        rustc_hir::Node::Stmt(rustc_hir::Stmt {
            kind: rustc_hir::StmtKind::Let(local),
            ..
        }) => Some(local),
        _ => None,
    }
}

fn owns_one_body(expression: &Expr<'_>) -> bool {
    struct Bodies(usize);
    impl<'hir> rustc_hir::intravisit::Visitor<'hir> for Bodies {
        fn visit_expr(&mut self, expression: &'hir Expr<'hir>) {
            if matches!(expression.kind, rustc_hir::ExprKind::Closure(..)) {
                self.0 += 1;
            } else {
                rustc_hir::intravisit::walk_expr(self, expression);
            }
        }
    }
    let mut bodies = Bodies(0);
    rustc_hir::intravisit::Visitor::visit_expr(&mut bodies, expression);
    bodies.0 == 1
}
