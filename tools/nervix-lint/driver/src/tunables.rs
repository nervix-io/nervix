//! Repository tooling, outside the product layer order.
//! Owns: resolved numeric policy recognition outside declared settings, derivations from typed
//! input and fixed-value contracts.
//! Depends on: compiler definitions, normalized types and the `constant` source contract.
//! Must not know: which product value is a setting, product filenames or runtime defaults.

use ahash::{HashSet, HashSetExt as _};
use rustc_ast::LitKind;
use rustc_hir::{
    Expr, ExprKind, HirId, ImplItem, ImplItemKind, Item, ItemKind, QPath, TraitItem, TraitItemKind,
    def::{DefKind, Res},
    intravisit::{self, Visitor},
};
use rustc_lint::LateContext;
use rustc_lint_defs::declare_tool_lint;
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::{
    Span, Symbol,
    def_id::{DefId, LOCAL_CRATE, LocalDefId},
};

use crate::contracts::{Flow, contract_attribute, meta_items};

declare_tool_lint! { pub nervix::HIDDEN_TUNABLE, Deny, "numeric policy belongs to a class-typed setting, a derivation from typed input or a fixed-value contract", @report_in_external_macro = true }

/// Generators, benchmarks, model checks and test services: their numbers shape test inputs and
/// measurements, never node or client policy.
const EXCLUDED_CRATES: [&str; 4] = [
    "nervix_arbitrary",
    "nervix_benchmark",
    "nervix_model_harness",
    "nervix_test_environment",
];

fn excluded(tcx: TyCtxt<'_>) -> bool {
    let name = tcx.crate_name(LOCAL_CRATE);
    EXCLUDED_CRATES.contains(&name.as_str())
}

/// The governed numeric types, resolved through aliases, imports and reexports.
fn governed<'tcx>(cx: &LateContext<'tcx>, value: Ty<'tcx>) -> bool {
    let value = cx
        .tcx
        .normalize_erasing_regions(cx.typing_env(), ty::Unnormalized::new_wip(value));
    match value.kind() {
        ty::Uint(width) => matches!(
            width,
            ty::UintTy::U8
                | ty::UintTy::U16
                | ty::UintTy::U32
                | ty::UintTy::U64
                | ty::UintTy::Usize
        ),
        ty::Int(width) => matches!(width, ty::IntTy::I32 | ty::IntTy::I64),
        ty::Float(width) => matches!(width, ty::FloatTy::F64),
        ty::Adt(adt, _) => {
            let path = crate::definition_path(cx.tcx, adt.did());
            if matches!(
                path.as_str(),
                "core::time::Duration" | "core::num::nonzero::NonZero"
            ) {
                return true;
            }
            cx.tcx.crate_name(adt.did().krate).as_str() == "ubyte"
                && cx.tcx.item_name(adt.did()).as_str() == "ByteUnit"
        }
        _ => false,
    }
}

/// Whether the constant carries the `fixed` kind of the constant contract. The contract's
/// placement, reason and uniqueness are validated by the shared contract validation, which
/// fails the build as `nervix::invalid_contract` for every malformed or misplaced form.
fn fixed(tcx: TyCtxt<'_>, definition: DefId) -> bool {
    let Some(attribute) = contract_attribute(tcx, definition, "constant") else {
        return false;
    };
    let Some(arguments) = meta_items(attribute) else {
        return false;
    };
    let Some(kind) = arguments.first().and_then(|argument| argument.meta_item()) else {
        return false;
    };
    kind.is_word() && kind.has_name(Symbol::intern("fixed"))
}

fn self_type<'tcx>(cx: &LateContext<'tcx>, implementation: DefId) -> Ty<'tcx> {
    cx.tcx
        .type_of(implementation)
        .instantiate_identity()
        .skip_norm_wip()
}

/// The trait a trait implementation implements; any other definition implements none.
fn implemented_trait(cx: &LateContext<'_>, implementation: DefId) -> Option<DefId> {
    if !matches!(
        cx.tcx.def_kind(implementation),
        DefKind::Impl { of_trait: true }
    ) {
        return None;
    }
    cx.tcx.impl_opt_trait_id(implementation)
}

/// `SettingDefinition::default_value` writes the one default of its setting.
fn setting_default(cx: &LateContext<'_>, function: DefId) -> bool {
    if cx.tcx.item_name(function).as_str() != "default_value" {
        return false;
    }
    let Some(implementation) = cx.tcx.opt_parent(function) else {
        return false;
    };
    let Some(definition) = implemented_trait(cx, implementation) else {
        return false;
    };
    matches!(
        crate::definition_path(cx.tcx, definition).as_str(),
        "nervix_models::settings::SettingDefinition"
            | "nervix_lint_fixtures::settings::SettingDefinition"
    )
}

/// `Default` of a nonempty struct whose every field is a `Setting<D>` writes its group defaults
/// through those settings' definitions.
fn setting_group_default(cx: &LateContext<'_>, implementation: DefId) -> bool {
    let Some(definition) = implemented_trait(cx, implementation) else {
        return false;
    };
    if !cx
        .tcx
        .is_diagnostic_item(Symbol::intern("Default"), definition)
    {
        return false;
    }
    let ty::Adt(group, _) = self_type(cx, implementation).kind() else {
        return false;
    };
    if !group.is_struct() {
        return false;
    }
    let fields = &group.non_enum_variant().fields;
    if fields.is_empty() {
        return false;
    }
    fields.iter().all(|field| {
        let value = cx
            .tcx
            .type_of(field.did)
            .instantiate_identity()
            .skip_norm_wip();
        let ty::Adt(setting, _) = value.kind() else {
            return false;
        };
        matches!(
            crate::definition_path(cx.tcx, setting.did()).as_str(),
            "nervix_models::settings::Setting" | "nervix_lint_fixtures::settings::Setting"
        )
    })
}

/// A definition inside a setting's own default or a setting group's defaults.
fn declares_setting_default(cx: &LateContext<'_>, mut definition: DefId) -> bool {
    loop {
        match cx.tcx.def_kind(definition) {
            DefKind::AssocFn if setting_default(cx, definition) => return true,
            DefKind::Impl { of_trait: true } if setting_group_default(cx, definition) => {
                return true;
            }
            _ => {}
        }
        let Some(parent) = cx.tcx.opt_parent(definition) else {
            return false;
        };
        definition = parent;
    }
}

/// Every associated constant that a local `SettingDefinition::default_value` reads from its own
/// definition's type: that setting's one default, written once beside the definition so a constant
/// context can name it too.
fn definition_defaults(cx: &LateContext<'_>) -> HashSet<DefId> {
    let mut defaults = HashSet::new();
    for item in cx.tcx.hir_crate_items(()).impl_items() {
        let function = item.owner_id.def_id;
        if !matches!(cx.tcx.def_kind(function), DefKind::AssocFn) {
            continue;
        }
        if !setting_default(cx, function.to_def_id()) {
            continue;
        }
        let Some(implementation) = cx.tcx.opt_parent(function.to_def_id()) else {
            continue;
        };
        let ty::Adt(definition, _) = self_type(cx, implementation).kind() else {
            continue;
        };
        let Some(body) = cx.tcx.hir_maybe_body_owned_by(function) else {
            continue;
        };
        let mut reads = DefaultReads {
            cx,
            typeck: cx.tcx.typeck(function),
            definition: definition.did(),
            defaults: &mut defaults,
        };
        reads.visit_expr(body.value);
    }
    defaults
}

/// The associated constants of one definition's type that its default reads.
struct DefaultReads<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    typeck: &'tcx ty::TypeckResults<'tcx>,
    definition: DefId,
    defaults: &'a mut HashSet<DefId>,
}

impl<'tcx> Visitor<'tcx> for DefaultReads<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if let ExprKind::Path(ref path) = expression.kind
            && let Res::Def(DefKind::AssocConst, constant) =
                self.typeck.qpath_res(path, expression.hir_id)
            && owner_type(self.cx, constant) == Some(self.definition)
        {
            self.defaults.insert(constant);
        }
        intravisit::walk_expr(self, expression);
    }
}

/// A constant of a governed type, counted once with its whole initializer.
fn governed_constant(cx: &LateContext<'_>, definition: DefId) -> bool {
    if !matches!(
        cx.tcx.def_kind(definition),
        DefKind::Const | DefKind::AssocConst
    ) {
        return false;
    }
    let value = cx
        .tcx
        .type_of(definition)
        .instantiate_identity()
        .skip_norm_wip();
    governed(cx, value)
}

/// The authored name of a definition. A derive or attribute macro can give a generated item an
/// authored span whose tokens are not that item's name; such an item is generated, not written.
fn authored_name(cx: &LateContext<'_>, definition: DefId) -> Option<Span> {
    let span = cx.tcx.def_ident_span(definition)?;
    let source = cx.tcx.sess.source_map().span_to_snippet(span).ok()?;
    let name = cx.tcx.item_name(definition);
    (source.trim().trim_start_matches("r#") == name.as_str()).then_some(span)
}

/// The constants of the crate being checked that state numeric policy.
#[derive(Default)]
pub struct TunableRules {
    /// The setting definitions' own defaults, found once per crate on first use.
    definition_defaults: Option<HashSet<DefId>>,
}

impl TunableRules {
    pub fn item<'tcx>(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>, flow: &mut Flow) {
        if matches!(item.kind, ItemKind::Const(..)) {
            self.constant(cx, item.owner_id.def_id, item.hir_id(), flow);
        }
    }

    pub fn impl_item<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        item: &'tcx ImplItem<'tcx>,
        flow: &mut Flow,
    ) {
        if matches!(item.kind, ImplItemKind::Const(..)) {
            self.constant(cx, item.owner_id.def_id, item.hir_id(), flow);
        }
    }

    /// A trait constant is a decision only where the trait supplies its value.
    pub fn trait_item<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        item: &'tcx TraitItem<'tcx>,
        flow: &mut Flow,
    ) {
        if let TraitItemKind::Const(_, Some(_), ..) = item.kind {
            self.constant(cx, item.owner_id.def_id, item.hir_id(), flow);
        }
    }

    /// Whether `definition` is the default its definition's `default_value` returns.
    fn definition_default(&mut self, cx: &LateContext<'_>, definition: DefId) -> bool {
        if !matches!(cx.tcx.def_kind(definition), DefKind::AssocConst) {
            return false;
        }
        let defaults = self
            .definition_defaults
            .get_or_insert_with(|| definition_defaults(cx));
        defaults.contains(&definition)
    }

    fn constant(
        &mut self,
        cx: &LateContext<'_>,
        definition: LocalDefId,
        node: HirId,
        flow: &mut Flow,
    ) {
        if excluded(cx.tcx) {
            return;
        }
        let definition = definition.to_def_id();
        if !governed_constant(cx, definition)
            || fixed(cx.tcx, definition)
            || declares_setting_default(cx, definition)
            || self.definition_default(cx, definition)
        {
            return;
        }
        diagnose_constant(cx, definition, node, flow);
    }
}

fn diagnose_constant(cx: &LateContext<'_>, definition: DefId, node: HirId, flow: &mut Flow) {
    let Some(span) = authored_name(cx, definition) else {
        return;
    };
    flow.diagnostic(
        cx,
        node,
        span,
        HIDDEN_TUNABLE,
        format!(
            "numeric constant `{}` is policy outside a declared setting: make it a class-typed \
             setting, derive it from typed input, or state what fixes it with \
             `nervix::constant(fixed, reason = ...)`",
            cx.tcx.item_name(definition)
        ),
    );
}

/// What a numeric argument chooses at a recognized policy call.
#[derive(Clone, Copy)]
enum Policy {
    Duration,
    Capacity,
    Channel,
    Permits,
}

impl Policy {
    fn describe(self) -> &'static str {
        match self {
            Self::Duration => "a duration",
            Self::Capacity => "a capacity",
            Self::Channel => "a channel capacity",
            Self::Permits => "a permit count",
        }
    }
}

fn owner_type(cx: &LateContext<'_>, definition: DefId) -> Option<DefId> {
    let implementation = cx.tcx.inherent_impl_of_assoc(definition)?;
    let ty::Adt(owner, _) = self_type(cx, implementation).kind() else {
        return None;
    };
    Some(owner.did())
}

fn policy(cx: &LateContext<'_>, definition: DefId) -> Option<Policy> {
    let name = cx.tcx.item_name(definition);
    let name = name.as_str();
    if name.starts_with("with_capacity") {
        return Some(Policy::Capacity);
    }
    if name == "add_permits" {
        return Some(Policy::Permits);
    }
    if matches!(name, "bounded" | "sync_channel") {
        return Some(Policy::Channel);
    }
    if name == "channel" {
        // A watch channel's argument is its initial value, not a capacity.
        let module = cx.tcx.opt_parent(definition)?;
        if cx.tcx.item_name(module).as_str() == "watch" {
            return None;
        }
        return Some(Policy::Channel);
    }
    let owner = owner_type(cx, definition)?;
    if crate::definition_path(cx.tcx, owner) == "core::time::Duration"
        && (name.starts_with("from_") || name == "new")
    {
        return Some(Policy::Duration);
    }
    if name == "new" && matches!(cx.tcx.item_name(owner).as_str(), "Semaphore" | "ArrayQueue") {
        return Some(Policy::Permits);
    }
    None
}

struct Literal {
    found: bool,
}

impl<'tcx> Visitor<'tcx> for Literal {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if let ExprKind::Lit(literal) = expression.kind
            && matches!(literal.node, LitKind::Int(..) | LitKind::Float(..))
        {
            self.found = true;
            return;
        }
        intravisit::walk_expr(self, expression);
    }
}

fn numeric_literal(arguments: &[Expr<'_>]) -> bool {
    let mut probe = Literal { found: false };
    for argument in arguments {
        probe.visit_expr(argument);
    }
    probe.found
}

struct PolicyCall<'tcx> {
    definition: DefId,
    span: Span,
    arguments: &'tcx [Expr<'tcx>],
}

fn callee<'tcx>(cx: &LateContext<'tcx>, expression: &'tcx Expr<'tcx>) -> Option<PolicyCall<'tcx>> {
    let typeck = cx.typeck_results();
    let call = match expression.kind {
        ExprKind::MethodCall(segment, _, arguments, _) => PolicyCall {
            definition: typeck.type_dependent_def_id(expression.hir_id)?,
            span: segment.ident.span,
            arguments,
        },
        ExprKind::Call(function, arguments) => {
            let ty::FnDef(definition, _) = *typeck.expr_ty(function).kind() else {
                return None;
            };
            let segment = match function.kind {
                ExprKind::Path(QPath::Resolved(_, path)) => path.segments.last()?,
                ExprKind::Path(QPath::TypeRelative(_, segment)) => segment,
                _ => return None,
            };
            PolicyCall {
                definition,
                span: segment.ident.span,
                arguments,
            }
        }
        _ => return None,
    };
    // A derive can give generated calls an authored span; the callee token is the written name.
    let source = cx.tcx.sess.source_map().span_to_snippet(call.span).ok()?;
    let name = cx.tcx.item_name(call.definition);
    if source.trim().trim_start_matches("r#") != name.as_str() {
        return None;
    }
    Some(call)
}

pub fn expression<'tcx>(cx: &LateContext<'tcx>, expression: &'tcx Expr<'tcx>, flow: &mut Flow) {
    if excluded(cx.tcx) {
        return;
    }
    let Some(call) = callee(cx, expression) else {
        return;
    };
    let Some(policy) = policy(cx, call.definition) else {
        return;
    };
    if !numeric_literal(call.arguments) {
        return;
    }
    let owner = cx.tcx.hir_enclosing_body_owner(expression.hir_id);
    let root = cx.tcx.typeck_root_def_id(owner.to_def_id());
    // A governed constant is diagnosed once, at its name, with its whole initializer.
    if governed_constant(cx, root) || declares_setting_default(cx, root) {
        return;
    }
    flow.diagnostic(
        cx,
        expression.hir_id,
        call.span,
        HIDDEN_TUNABLE,
        format!(
            "a numeric literal chooses {} in `{}`: take it from a declared setting, derive it \
             from typed input, or name it as a constant that states what fixes it",
            policy.describe(),
            cx.tcx.def_path_str(call.definition)
        ),
    );
}
