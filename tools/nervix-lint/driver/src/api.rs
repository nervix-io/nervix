//! Repository tooling, outside the product layer order.
//! Owns: resolved integer/time arithmetic and Option/Result control-flow diagnostics.
//! Depends on: compiler definitions, normalized types and source-local lint expectations.
//! Must not know: method spelling as ownership, product filenames or runtime policy.

use rustc_hir::{Expr, ExprKind, HirId, Node, QPath, def::Res};
use rustc_lint::LateContext;
use rustc_lint_defs::declare_tool_lint;
use rustc_middle::ty::{self, Ty};
use rustc_span::{Span, Symbol, def_id::DefId};

use crate::contracts::Flow;

declare_tool_lint! { pub nervix::CLAMPED_ARITHMETIC, Deny, "clamping and wrapping require their actual arithmetic meaning", report_in_external_macro: true }
declare_tool_lint! { pub nervix::COMBINATOR_CONTROL_FLOW, Deny, "Option and Result decisions belong in explicit statements", report_in_external_macro: true }

struct Call<'tcx> {
    definition: DefId,
    span: Span,
    receiver: &'tcx Expr<'tcx>,
    arguments: &'tcx [Expr<'tcx>],
}

fn call<'tcx>(cx: &LateContext<'tcx>, expression: &'tcx Expr<'tcx>) -> Option<Call<'tcx>> {
    let typeck = cx.typeck_results();
    let (definition, span, token, receiver, arguments) = match expression.kind {
        ExprKind::MethodCall(segment, receiver, arguments, _) => (
            typeck.type_dependent_def_id(expression.hir_id)?,
            segment.ident.span,
            segment.ident.name,
            receiver,
            arguments,
        ),
        ExprKind::Call(function, arguments) => {
            let ty::FnDef(definition, _) = *typeck.expr_ty(function).kind() else {
                return None;
            };
            let segment = match function.kind {
                ExprKind::Path(QPath::Resolved(_, path)) => path.segments.last()?,
                ExprKind::Path(QPath::TypeRelative(_, segment)) => segment,
                _ => return None,
            };
            let (receiver, arguments) = arguments.split_first()?;
            (
                definition,
                segment.ident.span,
                segment.ident.name,
                receiver,
                arguments,
            )
        }
        _ => return None,
    };
    // Generated derives can carry an authored item span. Actual caller or macro-body tokens
    // retain the callee token's span, including aliases and bindings of function definitions.
    let source = cx.tcx.sess.source_map().span_to_snippet(span).ok()?;
    if source.trim().trim_start_matches("r#") != token.as_str() {
        return None;
    }
    Some(Call {
        definition,
        span,
        receiver,
        arguments,
    })
}

fn owner<'tcx>(cx: &LateContext<'tcx>, definition: DefId) -> Option<Ty<'tcx>> {
    let implementation = cx.tcx.inherent_impl_of_assoc(definition)?;
    let value = cx
        .tcx
        .type_of(implementation)
        .instantiate_identity()
        .skip_norm_wip();
    Some(
        cx.tcx
            .normalize_erasing_regions(cx.typing_env(), ty::Unnormalized::new_wip(value)),
    )
}

fn arithmetic(cx: &LateContext<'_>, definition: DefId) -> bool {
    let operation = cx.tcx.item_name(definition);
    if !operation.as_str().starts_with("saturating_")
        && !operation.as_str().starts_with("wrapping_")
    {
        return false;
    }
    let Some(value) = owner(cx, definition) else {
        return false;
    };
    let krate = cx.tcx.crate_name(definition.krate);
    if krate.as_str() != "core" && krate.as_str() != "std" {
        return false;
    }
    if value.is_integral() {
        return true;
    }
    let ty::Adt(adt, _) = value.kind() else {
        return false;
    };
    crate::definition_path(cx.tcx, adt.did()) == "core::time::Duration"
}

fn adapter(cx: &LateContext<'_>, definition: DefId) -> bool {
    let operation = cx.tcx.item_name(definition);
    if !matches!(
        operation.as_str(),
        "and_then"
            | "filter"
            | "map"
            | "map_err"
            | "map_or"
            | "map_or_else"
            | "ok_or"
            | "ok_or_else"
            | "or_else"
            | "unwrap_or"
            | "unwrap_or_default"
            | "unwrap_or_else"
    ) {
        return false;
    }
    let Some(value) = owner(cx, definition) else {
        return false;
    };
    let ty::Adt(adt, _) = value.kind() else {
        return false;
    };
    cx.tcx
        .is_diagnostic_item(Symbol::intern("Option"), adt.did())
        || cx
            .tcx
            .is_diagnostic_item(Symbol::intern("Result"), adt.did())
}

fn closure_statements(cx: &LateContext<'_>, expression: &Expr<'_>) -> bool {
    let ExprKind::Closure(closure) = expression.kind else {
        return false;
    };
    let body = cx.tcx.hir_body(closure.body);
    if let ExprKind::Block(block, _) = body.value.kind {
        return !block.stmts.is_empty();
    }
    false
}

fn parent_adapter(cx: &LateContext<'_>, node: HirId) -> bool {
    let Node::Expr(parent) = cx.tcx.parent_hir_node(node) else {
        return false;
    };
    if let Some(parent) = call(cx, parent) {
        return parent.receiver.hir_id == node && adapter(cx, parent.definition);
    }
    false
}

pub fn expression<'tcx>(cx: &LateContext<'tcx>, expression: &'tcx Expr<'tcx>, flow: &mut Flow) {
    if let Some(current) = call(cx, expression) {
        if arithmetic(cx, current.definition) {
            flow.diagnostic(
                cx,
                expression.hir_id,
                current.span,
                CLAMPED_ARITHMETIC,
                "the resolved integer/Duration API clamps or wraps; use checked arithmetic or \
                 expect this one operation with its actual clamping or modular meaning"
                    .into(),
            );
        }
        if !adapter(cx, current.definition) || parent_adapter(cx, expression.hir_id) {
            return;
        }
        let mut count = 0;
        let mut control = false;
        let mut next = Some(current);
        while let Some(current) = next {
            count += 1;
            let operation = cx.tcx.item_name(current.definition);
            control |= matches!(operation.as_str(), "map_or" | "map_or_else");
            control |= current
                .arguments
                .iter()
                .any(|argument| closure_statements(cx, argument));
            next = call(cx, current.receiver);
            if next
                .as_ref()
                .is_some_and(|value| !adapter(cx, value.definition))
            {
                next = None;
            }
        }
        if control || count > 1 {
            flow.diagnostic(
                cx,
                expression.hir_id,
                expression.span,
                COMBINATOR_CONTROL_FLOW,
                "the resolved Option/Result adapters branch or sequence steps; use match, if let, \
                 let else or named statements; single transformations and iterator pipelines \
                 remain supported"
                    .into(),
            );
        }
        return;
    }
    // A canonical API coerced to a callback/function pointer still has a resolved reference.
    // Inspect that reference before its definition is erased by the external callable type.
    if let ExprKind::Path(path) = expression.kind
        && let Res::Def(_, definition) = cx.qpath_res(&path, expression.hir_id)
    {
        if let Node::Expr(parent) = cx.tcx.parent_hir_node(expression.hir_id)
            && let ExprKind::Call(function, _) = parent.kind
            && function.hir_id == expression.hir_id
        {
            return;
        }
        if arithmetic(cx, definition) {
            flow.diagnostic(
                cx,
                expression.hir_id,
                expression.span,
                CLAMPED_ARITHMETIC,
                "this resolved arithmetic callback clamps or wraps; retain an explicit checked \
                 operation or state this callback's actual modular/clamping meaning"
                    .into(),
            );
        }
        if adapter(cx, definition)
            && matches!(
                cx.tcx.item_name(definition).as_str(),
                "map_or" | "map_or_else"
            )
        {
            flow.diagnostic(
                cx,
                expression.hir_id,
                expression.span,
                COMBINATOR_CONTROL_FLOW,
                "the resolved branch adapter remains a decision when used as a callback; express \
                 its arms with named statements"
                    .into(),
            );
        }
    }
}
