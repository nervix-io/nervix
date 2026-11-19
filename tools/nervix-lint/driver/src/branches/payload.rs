//! Repository tooling, outside the product layer order.
//! Owns: resolved uses of branch key columns carried with record rows.
//! Depends on: compiler binding uses and the row contracts that consume those columns.
//! Must not know: source filenames or how an execution node owns its branches.

use rustc_hir::{
    BodyId, Expr, ExprKind, HirId, LetStmt, Node, PatKind, QPath,
    def::Res,
    intravisit::{self, Visitor},
};
use rustc_lint::LateContext;
use rustc_middle::{ty, ty::TyCtxt};

use crate::definition_path;

pub(super) fn field(owner: &str, field: &str) -> bool {
    field == "keys"
        && matches!(
            owner,
            "nervix_server::runtime::relay_batch::RelayRecordBatch"
                | "nervix_server::runtime::relay_batch::UnkeyedRelayBatchParts"
                | "nervix_server::runtime::ingest_group::BranchedEntrypointBatch"
                | "nervix_server::runtime::correlator::CorrelatorMatchedBatch"
                | "nervix_server::runtime::vm_input::VmInputProjectionSources"
                | "nervix_server::runtime::filter_map::FilterMapBatchInputs"
                | "nervix_server::runtime::filter_map::FilterMapOutcomeInputs"
        )
}

pub(super) fn carries_record_keys<'tcx>(cx: &LateContext<'tcx>, local: &LetStmt<'tcx>) -> bool {
    local_column(cx.tcx, local)
}

fn local_column<'tcx>(tcx: TyCtxt<'tcx>, local: &LetStmt<'tcx>) -> bool {
    let PatKind::Binding(_, binding, _, None) = local.pat.kind else {
        return false;
    };
    let Some(body) = tcx.hir_maybe_body_owned_by(local.hir_id.owner.def_id) else {
        return false;
    };
    let mut uses = Uses {
        tcx,
        binding,
        found: false,
        other: false,
    };
    uses.visit_body(body);
    uses.found && !uses.other
}

struct Uses<'tcx> {
    tcx: TyCtxt<'tcx>,
    binding: HirId,
    found: bool,
    other: bool,
}

impl<'tcx> Visitor<'tcx> for Uses<'tcx> {
    fn visit_nested_body(&mut self, body: BodyId) {
        self.visit_body(self.tcx.hir_body(body));
    }

    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if let ExprKind::Path(QPath::Resolved(None, path)) = expression.kind
            && path.res == Res::Local(self.binding)
        {
            match used_as_row_column(self.tcx, expression.hir_id) {
                Some(row) => self.found |= row,
                None => self.other = true,
            }
        }
        if !self.other {
            intravisit::walk_expr(self, expression);
        }
    }
}

fn used_as_row_column(tcx: TyCtxt<'_>, mut value: HirId) -> Option<bool> {
    let mut parents = tcx.hir_parent_iter(value);
    let mut projected_row = false;
    while let Some((id, parent)) = parents.next() {
        match parent {
            Node::Expr(Expr {
                kind: ExprKind::AddrOf(..) | ExprKind::DropTemps(..),
                ..
            }) => {
                value = id;
            }
            Node::Expr(Expr {
                kind: ExprKind::Index(column, ..),
                ..
            }) if column.hir_id == value => {
                value = id;
                projected_row = true;
            }
            Node::Expr(Expr {
                kind: ExprKind::MethodCall(_, receiver, [], _),
                ..
            }) if projected_row && receiver.hir_id == value && copies_row_key(tcx, id) => {
                value = id;
            }
            Node::Expr(Expr {
                kind: ExprKind::MethodCall(_, receiver, [_], _),
                ..
            }) if receiver.hir_id == value && appends_row_key(tcx, id) => {
                return Some(false);
            }
            Node::Block(block) if block.expr.is_some_and(|tail| tail.hir_id == value) => {
                value = id;
            }
            Node::Arm(arm) if arm.body.hir_id == value => {
                value = id;
            }
            Node::Expr(Expr {
                kind: ExprKind::Block(..),
                ..
            }) => {
                value = id;
            }
            Node::Expr(Expr {
                kind: ExprKind::Match(subject, ..),
                ..
            }) if subject.hir_id != value => {
                value = id;
            }
            Node::LetStmt(local) if local.init.is_some_and(|init| init.hir_id == value) => {
                return local_column(tcx, local).then_some(true);
            }
            Node::ExprField(column) => {
                let Some((_, Node::Expr(record))) = parents.next() else {
                    return None;
                };
                let ty::Adt(owner, _) = tcx
                    .typeck(record.hir_id.owner.def_id)
                    .expr_ty(record)
                    .kind()
                else {
                    return None;
                };
                let owner = definition_path(tcx, owner.did());
                let payload = field(&owner, column.ident.name.as_str())
                    || projected_row
                        && owner == "nervix_server::runtime::relay_batch::RelayMessage"
                        && column.ident.name.as_str() == "key"
                    || owner == "nervix_server::runtime::branch_runtime::BranchRuntime"
                        && column.ident.name.as_str() == "relays";
                return payload.then_some(true);
            }
            Node::Expr(Expr {
                kind: ExprKind::Call(callee, arguments),
                ..
            }) => {
                let ExprKind::Path(QPath::Resolved(None, path)) = callee.kind else {
                    return None;
                };
                let Res::Def(_, callee) = path.res else {
                    return None;
                };
                let position = match definition_path(tcx, callee).as_str() {
                    "nervix_server::runtime::filter_map::evaluate_output_branch_program" => 4,
                    "nervix_server::runtime::processor_output::pending_output_batches_by_key" => 2,
                    _ => return None,
                };
                return arguments
                    .get(position)
                    .is_some_and(|arg| arg.hir_id == value)
                    .then_some(true);
            }
            _ => return None,
        }
    }
    None
}

fn appends_row_key(tcx: TyCtxt<'_>, call: HirId) -> bool {
    tcx.typeck(call.owner.def_id)
        .type_dependent_def_id(call)
        .is_some_and(|definition| {
            definition_path(tcx, definition).starts_with("alloc::vec::")
                && tcx.item_name(definition).as_str() == "push"
        })
}

fn copies_row_key(tcx: TyCtxt<'_>, call: HirId) -> bool {
    tcx.typeck(call.owner.def_id)
        .type_dependent_def_id(call)
        .is_some_and(|definition| {
            let trait_item = tcx
                .opt_associated_item(definition)
                .and_then(|item| item.trait_item_def_id())
                .unwrap_or(definition);
            definition_path(tcx, trait_item) == "core::clone::Clone::clone"
        })
}
