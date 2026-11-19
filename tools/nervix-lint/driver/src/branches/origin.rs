//! Repository tooling, outside the product layer order.
//! Owns: resolution of a branch lifetime number still represented as an integer.
//! Depends on: the compiler's field definitions and local value sources.
//! Must not know: branch membership or the shape of a collection that would use the number.

use rustc_hir::{Expr, ExprKind, Node, PatKind, QPath, def::Res};
use rustc_middle::ty::{self, TyCtxt};

use crate::definition_path;

pub(super) fn branch_number<'tcx>(tcx: TyCtxt<'tcx>, mut value: &'tcx Expr<'tcx>) -> bool {
    loop {
        match value.kind {
            ExprKind::AddrOf(_, _, inner)
            | ExprKind::Cast(inner, _)
            | ExprKind::DropTemps(inner) => value = inner,
            ExprKind::Field(base, _) => return lifetime_field(tcx, value, base),
            ExprKind::Path(QPath::Resolved(None, path)) => {
                let Res::Local(binding) = path.res else {
                    return false;
                };
                let Some(Node::LetStmt(local)) = tcx
                    .hir_parent_iter(binding)
                    .map(|(_, node)| node)
                    .find(|node| matches!(node, Node::LetStmt(_)))
                else {
                    return false;
                };
                let PatKind::Binding(mode, local_binding, _, None) = local.pat.kind else {
                    return false;
                };
                if local_binding != binding || mode != rustc_hir::BindingMode::NONE {
                    return false;
                }
                let Some(init) = local.init else { return false };
                value = init;
            }
            _ => return false,
        }
    }
}

fn lifetime_field<'tcx>(tcx: TyCtxt<'tcx>, value: &Expr<'tcx>, base: &Expr<'tcx>) -> bool {
    let typeck = tcx.typeck(value.hir_id.owner.def_id);
    let ty::Adt(owner, arguments) = typeck.expr_ty_adjusted(base).peel_refs().kind() else {
        return false;
    };
    if !owner.is_struct() {
        return false;
    }
    let field = &owner.non_enum_variant().fields[typeck.field_index(value.hir_id)];
    matches!(
        definition_path(tcx, field.did).as_str(),
        "nervix_branch_instances::registry::BranchInstanceSnapshotEntry::incarnation"
            | "nervix_branch_instances::registry::BranchInstanceEntry::incarnation"
            | "nervix_lint_fixtures::BranchSnapshot::incarnation"
    ) && matches!(
        field.ty(tcx, arguments).skip_norm_wip().kind(),
        ty::Uint(_) | ty::Int(_)
    )
}
