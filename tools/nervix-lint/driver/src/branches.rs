//! Repository tooling, outside the product layer order.
//! Owns: resolved branch collection diagnostics.
//! Depends on: compiler types and the branch identity and state definitions.
//! Must not know: the spelling of a use, source filenames or runtime execution.

use ahash::{HashMap, HashSet, HashSetExt as _};
use rustc_hir::{Expr, ExprKind, FieldDef, HirId, LetStmt, PatKind, def::Res};
use rustc_lint::LateContext;
use rustc_lint_defs::declare_tool_lint;
use rustc_middle::ty::{self, Ty};
use rustc_span::{
    Span,
    def_id::{DefId, LOCAL_CRATE},
    hygiene::{ExpnKind, MacroKind},
};

use crate::{contracts::Flow, definition_path};

mod origin;
mod payload;

declare_tool_lint! { pub nervix::BRANCH_INVERSION, Deny, "a branch owns its runtime and the parametrizer partition owns its membership", report_in_external_macro: true }

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum BranchRole {
    Key,
    Part,
}

#[derive(Clone, Copy)]
enum Collection {
    Keyed,
    Indexed,
}

#[derive(Clone, Copy)]
struct Definition {
    collection: Option<Collection>,
    branch: Option<BranchRole>,
    wrapper: bool,
}

impl Definition {
    fn of(path: &str) -> Self {
        let collection = match path {
            "std::collections::hash::map::HashMap"
            | "alloc::collections::btree::map::BTreeMap"
            | "ahash::hash_map::AHashMap"
            | "dashmap::DashMap"
            | "nervix_primitives::collections::scheduled::DashMap"
            | "shuttle_dashmap_impl::DashMap"
            | "hashbrown::map::HashMap"
            | "indexmap::map::IndexMap"
            | "imbl::hash::map::GenericHashMap"
            | "imbl::ord::map::GenericOrdMap"
            | "rpds::map::hash_trie_map::HashTrieMap"
            | "rpds::map::red_black_tree_map::RedBlackTreeMap"
            | "nervix_branch_instances::registry::BranchInstanceRegistry"
            | "nervix_branch_instances::membership::OwnedBranches" => Some(Collection::Keyed),
            "alloc::vec::Vec"
            | "alloc::collections::vec_deque::VecDeque"
            | "alloc::collections::linked_list::LinkedList"
            | "alloc::collections::binary_heap::BinaryHeap"
            | "std::collections::hash::set::HashSet"
            | "alloc::collections::btree::set::BTreeSet"
            | "ahash::hash_set::AHashSet"
            | "hashbrown::set::HashSet"
            | "indexmap::set::IndexSet"
            | "imbl::hash::set::GenericHashSet"
            | "imbl::ord::set::GenericOrdSet"
            | "rpds::set::hash_trie_set::HashTrieSet"
            | "rpds::set::red_black_tree_set::RedBlackTreeSet"
            | "slab::Slab"
            | "sorted_vec::SortedVec"
            | "sorted_vec::SortedSet"
            | "nervix_branch_instances::membership::BranchPresence"
            | "nervix_branch_instances::membership::BranchMembership" => Some(Collection::Indexed),
            _ => None,
        };
        let branch = match path {
            "nervix_server::runtime::branch_key::BranchKey"
            | "nervix_server::runtime::branch_key::BranchScope"
            | "nervix_server::metrics::BranchInstanceMetricKey"
            | "nervix_server::runtime::backup_state::BranchStateIdentity"
            | "nervix_server::runtime::state_store::RuntimeStatePlacement"
            | "nervix_models::quiesce::impact::BranchKeyFingerprint"
            | "nervix_models::remote_branch_key::BranchKeyFields"
            | "nervix_models::remote_branch_key::InheritedBranchKeyFields"
            | "nervix_lint_fixtures::BranchKey"
            | "nervix_lint_fixtures::BranchIncarnation" => Some(BranchRole::Key),
            "nervix_server::runtime::branch_runtime::BranchRuntime"
            | "nervix_server::runtime::generator::GeneratorBranchTaskState"
            | "nervix_server::runtime::generator::GeneratorRouteBranchTaskState"
            | "nervix_server::runtime::materialized_state::MaterializedBranchRecord"
            | "nervix_server::runtime::processor_branch_task::ProcessorBranchTask"
            | "nervix_server::runtime::processor_partition_moves::ProcessorBranchSlot"
            | "nervix_server::runtime::reingestor::ReingestorBranchOutputBuffer"
            | "nervix_server::runtime::relay_boundary::ConcreteRelayRuntime"
            | "nervix_server::runtime::relay_channels::RelayBranchChannels"
            | "nervix_lint_fixtures::BranchTask" => Some(BranchRole::Part),
            _ => None,
        };
        let wrapper = matches!(
            path,
            "alloc::boxed::Box"
                | "alloc::sync::Arc"
                | "core::option::Option"
                | "core::result::Result"
                | "triomphe::arc::Arc"
        );
        Self {
            collection,
            branch,
            wrapper,
        }
    }
}

#[derive(Default)]
pub struct BranchRules {
    definitions: HashMap<DefId, Definition>,
}

impl BranchRules {
    fn definition(&mut self, cx: &LateContext<'_>, definition: DefId) -> Definition {
        *self
            .definitions
            .entry(definition)
            .or_insert_with(|| Definition::of(&definition_path(cx.tcx, definition)))
    }

    pub fn field(&mut self, cx: &LateContext<'_>, field: &FieldDef<'_>, flow: &mut Flow) -> bool {
        if Self::derived(field.span) {
            return false;
        }
        let value = cx
            .tcx
            .type_of(field.def_id)
            .instantiate_identity()
            .skip_norm_wip();
        if Self::partition_storage(cx, field) || self.owned_row_column(cx, field, value) {
            return false;
        }
        self.diagnose(cx, field.hir_id, field.ty.span, value, flow)
    }

    pub fn item(
        &mut self,
        cx: &LateContext<'_>,
        item: &rustc_hir::Item<'_>,
        flow: &mut Flow,
    ) -> Option<(String, Span)> {
        let (name, syntax) = match item.kind {
            rustc_hir::ItemKind::Static(_, name, syntax, _)
            | rustc_hir::ItemKind::Const(name, _, syntax, _) => (name, syntax),
            _ => return None,
        };
        let value = cx
            .tcx
            .type_of(item.owner_id.def_id)
            .instantiate_identity()
            .skip_norm_wip();
        self.diagnose(cx, item.hir_id(), syntax.span, value, flow)
            .then(|| (name.to_string(), syntax.span))
    }

    /// The three declarations assigned to the parametrizer partition. The mechanism's generic
    /// type is still diagnosable when instantiated outside its designated owner.
    fn partition_storage(cx: &LateContext<'_>, field: &FieldDef<'_>) -> bool {
        let owner = definition_path(cx.tcx, cx.tcx.parent(field.def_id.to_def_id()));
        matches!(
            (owner.as_str(), field.ident.name.as_str()),
            (
                "nervix_server::runtime::parametrizer_partition::ParametrizerPartition"
                    | "nervix_lint_fixtures::branch_ownership::ParametrizerPartition",
                "registry"
            ) | (
                "nervix_server::runtime::parametrizer_partition::BranchTable"
                    | "nervix_lint_fixtures::branch_ownership::BranchTable",
                "branches"
            ) | (
                "nervix_server::runtime::parametrizer_partition::BranchTableView"
                    | "nervix_lint_fixtures::branch_ownership::BranchTableView",
                "branches"
            )
        )
    }

    fn owned_row_column(
        &mut self,
        cx: &LateContext<'_>,
        field: &FieldDef<'_>,
        value: Ty<'_>,
    ) -> bool {
        let ty::Adt(container, arguments) = value.kind() else {
            return false;
        };
        let owner = cx.tcx.parent(field.def_id.to_def_id());
        let owner = definition_path(cx.tcx, owner);
        if payload::field(&owner, field.ident.name.as_str()) {
            return definition_path(cx.tcx, container.did()) == "alloc::vec::Vec";
        }
        // These owners are already one concrete branch. They index its own parts by relay or
        // route, with no branch identity in the collection's key.
        match (owner.as_str(), field.ident.name.as_str()) {
            ("nervix_server::runtime::branch_runtime::BranchRuntime", "relays") => {
                matches!(
                    self.definition(cx, container.did()).collection,
                    Some(Collection::Keyed)
                ) && matches!(arguments.type_at(0).kind(), ty::Adt(key, _)
                        if definition_path(cx.tcx, key.did()) == "nervix_models::names::RelayName")
                    && matches!(arguments.type_at(1).kind(), ty::Adt(part, _)
                        if definition_path(cx.tcx, part.did()) ==
                            "nervix_server::runtime::relay_boundary::ConcreteRelayRuntime")
            }
            ("nervix_server::runtime::generator::GeneratorBranchTaskState", "routes") => {
                definition_path(cx.tcx, container.did()) == "alloc::vec::Vec"
                    && matches!(arguments.type_at(0).kind(), ty::Adt(part, _)
                        if definition_path(cx.tcx, part.did()) ==
                            "nervix_server::runtime::generator::GeneratorRouteBranchTaskState")
            }
            _ => false,
        }
    }

    pub fn binding<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        local: &LetStmt<'tcx>,
        flow: &mut Flow,
    ) -> Option<Span> {
        if Self::derived(local.span)
            || cx.tcx.crate_name(LOCAL_CRATE).as_str() == "nervix_arbitrary"
        {
            return None;
        }
        let init = local.init?;
        if !Self::creates_value(cx, init) || self.initializes_returned_field(cx, local) {
            return None;
        }
        let value = cx.typeck_results().pat_ty(local.pat);
        if !self.inverted(cx, value) || payload::carries_record_keys(cx, local) {
            return None;
        }
        let span = match local.ty {
            Some(annotation) => annotation.span,
            None => local.pat.span,
        };
        Self::emit(cx, local.hir_id, span, flow);
        Some(span)
    }

    /// A constructor can assemble a field in a local before returning the new owner. The field
    /// is the declaration of that collection. Count a local separately if it is not the value
    /// moved directly into a field that the compiler visits in the defining type.
    fn initializes_returned_field(&mut self, cx: &LateContext<'_>, local: &LetStmt<'_>) -> bool {
        let PatKind::Binding(_, binding, _, None) = local.pat.kind else {
            return false;
        };
        let Some(body) = cx.tcx.hir_maybe_body_owned_by(local.hir_id.owner.def_id) else {
            return false;
        };
        let Some(tail) = Self::returned_record(cx, body.value) else {
            return false;
        };
        let ExprKind::Struct(path, fields, _) = tail.kind else {
            return false;
        };
        let ty::Adt(adt, _) = cx.typeck_results().expr_ty(tail).kind() else {
            return false;
        };
        if !adt.did().is_local() {
            return false;
        }
        let variant = adt.variant_of_res(cx.qpath_res(path, tail.hir_id));
        fields.iter().any(|field| {
            let ExprKind::Path(path) = field.expr.kind else {
                return false;
            };
            if cx.qpath_res(&path, field.expr.hir_id) != Res::Local(binding) {
                return false;
            }
            let definition = &variant.fields[cx.typeck_results().field_index(field.hir_id)];
            let value = cx
                .tcx
                .type_of(definition.did)
                .instantiate_identity()
                .skip_norm_wip();
            self.holds_branch_collection(cx, value, &mut HashSet::new())
        })
    }

    fn returned_record<'tcx>(
        cx: &LateContext<'tcx>,
        mut expression: &'tcx Expr<'tcx>,
    ) -> Option<&'tcx Expr<'tcx>> {
        loop {
            match expression.kind {
                ExprKind::Struct(..) => return Some(expression),
                ExprKind::Block(block, _) => expression = block.expr?,
                ExprKind::Call(callee, [value]) => {
                    let ExprKind::Path(path) = callee.kind else {
                        return None;
                    };
                    let Res::Def(_, definition) = cx.qpath_res(&path, callee.hir_id) else {
                        return None;
                    };
                    let owner = cx.tcx.opt_parent(cx.tcx.parent(definition))?;
                    if !matches!(
                        definition_path(cx.tcx, owner).as_str(),
                        "core::result::Result" | "core::option::Option"
                    ) {
                        return None;
                    }
                    expression = value;
                }
                _ => return None,
            }
        }
    }

    fn derived(span: Span) -> bool {
        let mut context = span.ctxt();
        while !context.is_root() {
            let expansion = context.outer_expn_data();
            if let ExpnKind::Macro(MacroKind::Derive, _) = expansion.kind {
                return true;
            }
            context = expansion.call_site.ctxt();
        }
        false
    }

    /// Count a declaration that makes a collection. Moving one into another binding and
    /// borrowing an existing owner keep the original declaration.
    fn creates_value(cx: &LateContext<'_>, init: &Expr<'_>) -> bool {
        match init.kind {
            ExprKind::Array(..)
            | ExprKind::Repeat(..)
            | ExprKind::Call(..)
            | ExprKind::Struct(..) => true,
            ExprKind::MethodCall(..) => {
                let Some(method) = cx.typeck_results().type_dependent_def_id(init.hir_id) else {
                    return false;
                };
                let declaration = cx
                    .tcx
                    .opt_associated_item(method)
                    .and_then(|item| item.trait_item_def_id())
                    .unwrap_or(method);
                let owner = definition_path(cx.tcx, declaration);
                matches!(
                    owner.as_str(),
                    "core::iter::traits::iterator::Iterator::collect"
                        | "core::clone::Clone::clone"
                        | "alloc::borrow::ToOwned::to_owned"
                ) || owner.starts_with("alloc::slice::")
                    && cx.tcx.item_name(declaration).as_str() == "to_vec"
            }
            ExprKind::Block(block, _) => match block.expr {
                Some(tail) => Self::creates_value(cx, tail),
                None => false,
            },
            _ => false,
        }
    }

    fn diagnose<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        node: HirId,
        span: Span,
        value: Ty<'tcx>,
        flow: &mut Flow,
    ) -> bool {
        if !self.inverted(cx, value) {
            return false;
        }
        Self::emit(cx, node, span, flow);
        true
    }

    fn inverted<'tcx>(&mut self, cx: &LateContext<'tcx>, value: Ty<'tcx>) -> bool {
        let value = cx
            .tcx
            .try_normalize_erasing_regions(cx.typing_env(), ty::Unnormalized::new_wip(value))
            .unwrap_or(value);
        self.holds_branch_collection(cx, value, &mut HashSet::new())
    }

    pub fn numeric_identity_use<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        expression: &'tcx Expr<'tcx>,
        flow: &mut Flow,
    ) -> Option<Span> {
        if Self::derived(expression.span) {
            return None;
        }
        let (receiver, key) = match expression.kind {
            ExprKind::Index(receiver, key, _) => (receiver, key),
            ExprKind::Call(..) | ExprKind::MethodCall(..) => {
                let call = crate::api::call(cx, expression)?;
                let method = cx.tcx.item_name(call.definition);
                if cx.tcx.inherent_impl_of_assoc(call.definition).is_none()
                    || !matches!(
                        method.as_str(),
                        "insert"
                            | "entry"
                            | "get"
                            | "get_mut"
                            | "contains"
                            | "contains_key"
                            | "remove"
                            | "remove_entry"
                            | "push"
                    )
                {
                    return None;
                }
                (call.receiver, call.arguments.first()?)
            }
            _ => return None,
        };
        let typeck = cx.typeck_results();
        let ty::Adt(container, _) = typeck.expr_ty_adjusted(receiver).peel_refs().kind() else {
            return None;
        };
        if self.definition(cx, container.did()).collection.is_some()
            && matches!(
                typeck.expr_ty_adjusted(key).peel_refs().kind(),
                ty::Uint(_) | ty::Int(_)
            )
            && origin::branch_number(cx.tcx, key)
        {
            Self::emit(cx, expression.hir_id, key.span, flow);
            return Some(key.span);
        }
        None
    }

    fn emit(cx: &LateContext<'_>, node: HirId, span: Span, flow: &mut Flow) {
        flow.diagnostic(
            cx,
            node,
            span,
            BRANCH_INVERSION,
            "this collection keeps branch identities or branch-owned parts; retain the branch's \
             bound handle or go through its parametrizer partition"
                .into(),
        );
    }

    fn holds_branch_collection<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        value: Ty<'tcx>,
        visited: &mut HashSet<DefId>,
    ) -> bool {
        match value.kind() {
            ty::Adt(adt, arguments) => {
                let definition = self.definition(cx, adt.did());
                if definition.branch.is_some() {
                    return false;
                }
                if let Some(collection) = definition.collection {
                    let first = arguments.type_at(0);
                    let first_is_branch = match collection {
                        Collection::Keyed => self.branch_key(cx, first, &mut HashSet::new()),
                        Collection::Indexed => self.branch_element(cx, first, &mut HashSet::new()),
                    };
                    if first_is_branch || self.holds_branch_collection(cx, first, visited) {
                        return true;
                    }
                    if let Collection::Keyed = collection {
                        let value = arguments.type_at(1);
                        return self.branch_element(cx, value, &mut HashSet::new())
                            || self.holds_branch_collection(cx, value, visited);
                    }
                    return false;
                }
                self.instantiated_fields(cx, *adt, arguments, visited)
            }
            ty::Tuple(values) => values
                .into_iter()
                .any(|value| self.holds_branch_collection(cx, value, visited)),
            ty::Array(element, _) | ty::Slice(element) => {
                self.branch_element(cx, *element, &mut HashSet::new())
                    || self.holds_branch_collection(cx, *element, visited)
            }
            _ => false,
        }
    }

    fn instantiated_fields<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        adt: ty::AdtDef<'tcx>,
        arguments: ty::GenericArgsRef<'tcx>,
        visited: &mut HashSet<DefId>,
    ) -> bool {
        if !visited.insert(adt.did()) {
            return false;
        }
        let mut result = false;
        let mut branch_argument = false;
        for argument in arguments.types() {
            if self.holds_branch_collection(cx, argument, visited) {
                result = true;
                break;
            }
            branch_argument |= self.branch_element(cx, argument, &mut HashSet::new());
        }
        if !result && branch_argument && Self::authored_type(cx, adt.did()) {
            result = adt.all_fields().any(|field| {
                let value = field.ty(cx.tcx, arguments).skip_norm_wip();
                self.holds_branch_collection(cx, value, visited)
            });
        }
        visited.remove(&adt.did());
        result
    }

    fn authored_type(cx: &LateContext<'_>, definition: DefId) -> bool {
        definition.is_local()
            || cx
                .tcx
                .crate_name(definition.krate)
                .as_str()
                .starts_with("nervix")
    }

    fn branch_key<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        key: Ty<'tcx>,
        visited: &mut HashSet<DefId>,
    ) -> bool {
        if self.branch_element(cx, key, &mut HashSet::new()) {
            return true;
        }
        let (adt, arguments) = match key.peel_refs().kind() {
            ty::Adt(adt, arguments) => (adt, arguments),
            ty::Tuple(values) => {
                return values
                    .into_iter()
                    .any(|value| self.branch_key(cx, value, visited));
            }
            _ => return false,
        };
        if definition_path(cx.tcx, adt.did())
            == "nervix_server::fault_injection::command_pause_point::CommandPausePoint"
        {
            // This test seam selects a command pause, including an optional reset target.
            return false;
        }
        if self.definition(cx, adt.did()).wrapper {
            return arguments
                .types()
                .any(|value| self.branch_key(cx, value, visited));
        }
        if !Self::authored_type(cx, adt.did()) || !visited.insert(adt.did()) {
            return false;
        }
        let found = adt.all_fields().any(|field| {
            let value = field.ty(cx.tcx, arguments).skip_norm_wip();
            self.branch_key(cx, value, visited)
        });
        visited.remove(&adt.did());
        found
    }

    fn branch_element<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        element: Ty<'tcx>,
        visited: &mut HashSet<DefId>,
    ) -> bool {
        self.element_role(cx, element, visited).is_some()
    }

    fn element_role<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        element: Ty<'tcx>,
        visited: &mut HashSet<DefId>,
    ) -> Option<BranchRole> {
        match element.kind() {
            ty::Adt(adt, arguments) => {
                let definition = self.definition(cx, adt.did());
                if let Some(role) = definition.branch {
                    return Some(role);
                }
                if !visited.insert(adt.did()) {
                    return None;
                }
                let found = if definition.wrapper {
                    arguments
                        .types()
                        .filter_map(|value| self.element_role(cx, value, visited))
                        .max()
                } else if Self::authored_type(cx, adt.did()) && adt.is_struct() {
                    // A newtype is the same branch item with another type name. A multi-field
                    // value may be a row or report carrying branch metadata alongside its data.
                    let mut fields = adt.non_enum_variant().fields.iter();
                    match (fields.next(), fields.next()) {
                        (Some(field), None) => self.element_role(
                            cx,
                            field.ty(cx.tcx, arguments).skip_norm_wip(),
                            visited,
                        ),
                        _ => None,
                    }
                } else {
                    None
                };
                visited.remove(&adt.did());
                match found {
                    Some(BranchRole::Key)
                        if definition.wrapper
                            && Self::is_branch_row_outcome(cx, *adt, arguments) =>
                    {
                        None
                    }
                    other => other,
                }
            }
            ty::Tuple(values) => {
                let role = values
                    .into_iter()
                    .filter_map(|value| self.element_role(cx, value, visited))
                    .max();
                match role {
                    Some(BranchRole::Key)
                        if values.iter().any(|value| Self::record_payload(cx, value)) =>
                    {
                        None
                    }
                    other => other,
                }
            }
            ty::Array(value, _)
            | ty::Slice(value)
            | ty::Ref(_, value, _)
            | ty::RawPtr(value, _) => self.element_role(cx, *value, visited),
            _ => None,
        }
    }

    fn is_branch_row_outcome(
        cx: &LateContext<'_>,
        adt: ty::AdtDef<'_>,
        arguments: ty::GenericArgsRef<'_>,
    ) -> bool {
        definition_path(cx.tcx, adt.did()) == "core::result::Result"
            && matches!(arguments.type_at(1).kind(), ty::Adt(error, _)
                if definition_path(cx.tcx, error.did()) ==
                    "nervix_server::runtime::filter_map::BranchRowError")
    }

    fn record_payload(cx: &LateContext<'_>, value: Ty<'_>) -> bool {
        let ty::Adt(adt, arguments) = value.kind() else {
            return false;
        };
        match definition_path(cx.tcx, adt.did()).as_str() {
            "nervix_models::remote::RemoteRuntimeRecordMetadata"
            | "nervix_server::runtime_schema::RuntimeRow"
            | "nervix_server::runtime::state_store::PersistedRuntimeStateEntry"
            | "nervix_lint_fixtures::PayloadRow" => true,
            "alloc::vec::Vec" => matches!(arguments.type_at(0).kind(), ty::Adt(row, _)
                if definition_path(cx.tcx, row.did()) ==
                    "nervix_server::runtime::relay_batch::RelayMessage"),
            _ => false,
        }
    }
}
