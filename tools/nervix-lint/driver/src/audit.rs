//! Repository tooling, outside the product layer order.
//! Owns: resolved review candidates for code shaped around inaccurate lints.
//! Depends on: HIR, type-check results, definition metadata and the report vocabulary.
//! Must not know: review decisions, approval records or product filenames.

use std::collections::BTreeMap;

use ahash::{HashSet, HashSetExt as _};
use nervix_lint_report::audit::{
    Construct, ErrorInformation, KeyedAccess, checked_operation, copies_value, fallback,
    keyed_access, preserves_value,
};
use rustc_attr_ir::LangItem;
use rustc_hir::{
    self as hir, Expr, ExprKind, HirId, MatchSource, PatKind, QPath, StmtKind, UnOp,
    def::{DefKind, Res},
    intravisit::{self, Visitor},
};
use rustc_lint::LateContext;
use rustc_middle::ty::{
    self, Ty, TypeVisitableExt as _, adjustment::Adjust, layout::LayoutOf as _,
};
use rustc_span::{
    Span, Symbol,
    def_id::{DefId, LocalDefId},
};

pub struct Operation {
    pub name: String,
    pub span: Span,
}

pub enum Pattern {
    RepeatedLookup {
        collection: String,
        receiver: String,
        key: String,
        guarded: bool,
        operations: Vec<Operation>,
    },
    KeyAllocation {
        collection: String,
        operation: String,
        allocation: String,
        key_type: String,
    },
    BorrowedCopy {
        operation: String,
        value_type: String,
    },
    DuplicatedBranch {
        construct: Construct,
        statement: String,
        occurrences: Vec<Span>,
    },
    ForwardingWrapper {
        callee: DefId,
        contract: bool,
        implementation: bool,
        relocated: bool,
    },
    DiscardedError {
        operation: String,
        error_type: String,
        information: ErrorInformation,
    },
    DisguisedClamp {
        operation: String,
        fallback: String,
    },
}

pub struct Observation {
    pub owner: LocalDefId,
    pub span: Span,
    pub pattern: Pattern,
}

/// Candidates collected while the compiler visits authored bodies.
#[derive(Default)]
pub struct Audit {
    pub observations: Vec<Observation>,
}

impl Audit {
    pub fn body<'tcx>(&mut self, cx: &LateContext<'tcx>, body: &hir::Body<'tcx>) {
        let owner = cx.tcx.hir_body_owner_def_id(body.id());
        // Derives generate data operations; their authored attribute span is not authored code.
        if let Some(parent) = cx.tcx.opt_parent(owner.to_def_id())
            && matches!(cx.tcx.def_kind(parent), DefKind::Impl { .. })
            && cx.tcx.is_automatically_derived(parent)
        {
            return;
        }
        let mut walker = Walker {
            cx,
            owner,
            steps: Vec::new(),
            keyed: Vec::new(),
            assignments: Vec::new(),
            observations: Vec::new(),
        };
        walker.visit_expr(body.value);
        walker.repeated_lookups();
        self.observations.append(&mut walker.observations);
        if let Some(observation) = forwarding_wrapper(cx, owner, body) {
            self.observations.push(observation);
        }
    }
}

/// The part of one conditional an expression evaluates in.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Region {
    Condition,
    Branch(usize),
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct Step {
    conditional: HirId,
    region: Region,
}

/// A value's resolved identity: the places it reads and how they are combined.
#[derive(Clone)]
struct Identity {
    /// Locals are named by their binding, so shadowing never merges two values.
    key: String,
    display: String,
    /// Places whose assignment changes this value.
    places: Vec<String>,
}

impl Identity {
    fn overlaps(&self, place: &str) -> bool {
        self.places.iter().any(|own| {
            own == place
                || own.starts_with(&format!("{place}."))
                || place.starts_with(&format!("{own}."))
        })
    }
}

struct Keyed {
    collection: String,
    operation: String,
    receiver: Identity,
    key: Identity,
    span: Span,
    steps: Vec<Step>,
}

struct Assignment {
    place: String,
    span: Span,
}

struct Statement {
    text: String,
    span: Span,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Group {
    receiver: String,
    key: String,
}

struct Walker<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    owner: LocalDefId,
    steps: Vec<Step>,
    keyed: Vec<Keyed>,
    assignments: Vec<Assignment>,
    observations: Vec<Observation>,
}

/// A method or path call with its resolved definition and receiver.
struct Call<'tcx> {
    definition: DefId,
    name: String,
    receiver: &'tcx Expr<'tcx>,
    arguments: &'tcx [Expr<'tcx>],
    span: Span,
}

fn exclusive(earlier: &[Step], later: &[Step]) -> bool {
    for first in earlier {
        for second in later {
            if first.conditional == second.conditional
                && let (Region::Branch(left), Region::Branch(right)) = (first.region, second.region)
                && left != right
            {
                return true;
            }
        }
    }
    false
}

fn guards(earlier: &[Step], later: &[Step]) -> bool {
    for first in earlier {
        if first.region != Region::Condition {
            continue;
        }
        for second in later {
            if second.conditional == first.conditional && matches!(second.region, Region::Branch(_))
            {
                return true;
            }
        }
    }
    false
}

fn normalized(text: &str) -> String {
    let words: Vec<&str> = text.split_whitespace().collect();
    words.join(" ").trim_end_matches(';').trim().to_owned()
}

impl<'tcx> Walker<'_, 'tcx> {
    fn snippet(&self, span: Span) -> Option<String> {
        self.cx.tcx.sess.source_map().span_to_snippet(span).ok()
    }

    fn adt_path(&self, value: Ty<'tcx>) -> Option<String> {
        let mut value = self
            .cx
            .tcx
            .normalize_erasing_regions(self.cx.typing_env(), ty::Unnormalized::new_wip(value));
        while let ty::Ref(_, referent, _) = value.kind() {
            value = *referent;
        }
        let ty::Adt(adt, _) = value.kind() else {
            return None;
        };
        Some(crate::definition_path(self.cx.tcx, adt.did()))
    }

    fn call(&self, expression: &'tcx Expr<'tcx>) -> Option<Call<'tcx>> {
        let typeck = self.cx.typeck_results();
        match expression.kind {
            ExprKind::MethodCall(segment, receiver, arguments, _) => {
                let definition = typeck.type_dependent_def_id(expression.hir_id)?;
                Some(Call {
                    definition,
                    name: segment.ident.name.to_string(),
                    receiver,
                    arguments,
                    span: expression.span,
                })
            }
            ExprKind::Call(function, arguments) => {
                let ty::FnDef(definition, _) = *typeck.expr_ty(function).kind() else {
                    return None;
                };
                let (receiver, arguments) = arguments.split_first()?;
                Some(Call {
                    definition,
                    name: self.cx.tcx.item_name(definition).to_string(),
                    receiver,
                    arguments,
                    span: expression.span,
                })
            }
            _ => None,
        }
    }

    /// The inherent owner of a resolved method, such as `HashMap` or `Result`.
    fn owner_path(&self, definition: DefId) -> Option<String> {
        let implementation = self.cx.tcx.inherent_impl_of_assoc(definition)?;
        let owner = self
            .cx
            .tcx
            .type_of(implementation)
            .instantiate_identity()
            .skip_norm_wip();
        self.adt_path(owner)
    }

    fn diagnostic_owner(&self, definition: DefId, name: &str) -> bool {
        let Some(implementation) = self.cx.tcx.inherent_impl_of_assoc(definition) else {
            return false;
        };
        let owner = self
            .cx
            .tcx
            .type_of(implementation)
            .instantiate_identity()
            .skip_norm_wip();
        let ty::Adt(adt, _) = owner.kind() else {
            return false;
        };
        self.cx
            .tcx
            .is_diagnostic_item(Symbol::intern(name), adt.did())
    }

    /// Method receivers that a call reads through a shared reference or copies.
    fn reads_receiver(&self, definition: DefId) -> bool {
        let signature = self
            .cx
            .tcx
            .fn_sig(definition)
            .instantiate_identity()
            .skip_norm_wip()
            .skip_binder();
        let Some(receiver) = signature.inputs().first() else {
            return false;
        };
        match receiver.kind() {
            ty::Ref(_, _, mutability) => !mutability.is_mut(),
            _ => self
                .cx
                .tcx
                .type_is_copy_modulo_regions(self.cx.typing_env(), *receiver),
        }
    }

    fn identity(&self, expression: &'tcx Expr<'tcx>) -> Option<Identity> {
        match expression.kind {
            ExprKind::AddrOf(_, _, inner)
            | ExprKind::Unary(UnOp::Deref, inner)
            | ExprKind::DropTemps(inner) => self.identity(inner),
            ExprKind::Path(ref path) => match self.cx.qpath_res(path, expression.hir_id) {
                Res::Local(binding) => {
                    let name = self.snippet(expression.span)?;
                    let key = format!("local:{binding:?}");
                    Some(Identity {
                        places: vec![key.clone()],
                        key,
                        display: name,
                    })
                }
                Res::Def(
                    DefKind::Const | DefKind::AssocConst | DefKind::Static { .. },
                    definition,
                ) => {
                    let key = crate::definition_path(self.cx.tcx, definition);
                    Some(Identity {
                        display: self.snippet(expression.span).unwrap_or_else(|| key.clone()),
                        key,
                        places: Vec::new(),
                    })
                }
                Res::SelfCtor(_) | Res::Def(DefKind::Ctor(..), _) => {
                    let text = self.snippet(expression.span)?;
                    Some(Identity {
                        key: format!("constructor:{text}"),
                        display: text,
                        places: Vec::new(),
                    })
                }
                _ => None,
            },
            ExprKind::Field(base, field) => {
                let base = self.identity(base)?;
                let key = format!("{}.{}", base.key, field.name);
                let places = if base.places.len() == 1 && base.places[0] == base.key {
                    vec![key.clone()]
                } else {
                    base.places
                };
                Some(Identity {
                    display: format!("{}.{}", base.display, field.name),
                    key,
                    places,
                })
            }
            ExprKind::Lit(_) => {
                let text = self.snippet(expression.span)?;
                Some(Identity {
                    key: format!("literal:{text}"),
                    display: text,
                    places: Vec::new(),
                })
            }
            ExprKind::Tup(items) => {
                let mut keys = Vec::new();
                let mut displays = Vec::new();
                let mut places = Vec::new();
                for item in items {
                    let identity = self.identity(item)?;
                    keys.push(identity.key);
                    displays.push(identity.display);
                    places.extend(identity.places);
                }
                Some(Identity {
                    key: format!("({})", keys.join(",")),
                    display: format!("({})", displays.join(", ")),
                    places,
                })
            }
            ExprKind::MethodCall(segment, receiver, [], _) => {
                let name = segment.ident.name.to_string();
                if preserves_value(&name) {
                    return self.identity(receiver);
                }
                let definition = self
                    .cx
                    .typeck_results()
                    .type_dependent_def_id(expression.hir_id)?;
                if !self.reads_receiver(definition) {
                    return None;
                }
                let base = self.identity(receiver)?;
                Some(Identity {
                    key: format!("{}.{name}()", base.key),
                    display: format!("{}.{name}()", base.display),
                    places: base.places,
                })
            }
            ExprKind::Index(base, index, _) => {
                let base = self.identity(base)?;
                let index = self.identity(index)?;
                let mut places = base.places;
                places.extend(index.places);
                Some(Identity {
                    key: format!("{}[{}]", base.key, index.key),
                    display: format!("{}[{}]", base.display, index.display),
                    places,
                })
            }
            _ => None,
        }
    }

    fn assignment(&mut self, target: &'tcx Expr<'tcx>, span: Span) {
        if let Some(identity) = self.identity(target) {
            for place in identity.places {
                self.assignments.push(Assignment { place, span });
            }
        }
    }

    fn keyed(&mut self, call: &Call<'tcx>) {
        let Some(collection) = self.owner_path(call.definition) else {
            return;
        };
        let Some(access) = keyed_access(&collection, &call.name) else {
            return;
        };
        let Some(key) = call.arguments.first() else {
            return;
        };
        self.key_allocation(&collection, access, call, key);
        let (Some(receiver), Some(key)) = (self.identity(call.receiver), self.identity(key)) else {
            return;
        };
        self.keyed.push(Keyed {
            collection,
            operation: call.name.clone(),
            receiver,
            key,
            span: call.span,
            steps: self.steps.clone(),
        });
    }

    fn index(
        &mut self,
        expression: &'tcx Expr<'tcx>,
        base: &'tcx Expr<'tcx>,
        index: &'tcx Expr<'tcx>,
    ) {
        let Some(collection) = self.adt_path(self.cx.typeck_results().expr_ty_adjusted(base))
        else {
            return;
        };
        if keyed_access(&collection, "index").is_none() {
            return;
        }
        let (Some(receiver), Some(key)) = (self.identity(base), self.identity(index)) else {
            return;
        };
        self.keyed.push(Keyed {
            collection,
            operation: "index".into(),
            receiver,
            key,
            span: expression.span,
            steps: self.steps.clone(),
        });
    }

    /// An allocation the call could avoid: an owned key for a borrowed lookup, or an owned key
    /// that an entry discards when the key is already present.
    fn key_allocation(
        &mut self,
        collection: &str,
        access: KeyedAccess,
        call: &Call<'tcx>,
        key: &'tcx Expr<'tcx>,
    ) {
        let built = match (access.borrows_key(), key.kind) {
            (true, ExprKind::AddrOf(_, _, inner)) => inner,
            (false, _) if access == KeyedAccess::Entry => key,
            _ => return,
        };
        let Some(allocation) = self.allocation(built) else {
            return;
        };
        let key_type = self.cx.typeck_results().expr_ty(built).to_string();
        self.observations.push(Observation {
            owner: self.owner,
            span: call.span,
            pattern: Pattern::KeyAllocation {
                collection: collection.to_owned(),
                operation: call.name.clone(),
                allocation,
                key_type,
            },
        });
    }

    /// Owned values built from existing ones: a copying conversion or a formatted string.
    fn allocation(&self, expression: &'tcx Expr<'tcx>) -> Option<String> {
        let value = self.cx.typeck_results().expr_ty(expression);
        if self
            .cx
            .tcx
            .type_is_copy_modulo_regions(self.cx.typing_env(), value)
        {
            return None;
        }
        if let Some(path) = self.adt_path(value)
            && matches!(
                path.as_str(),
                "alloc::sync::Arc" | "alloc::rc::Rc" | "triomphe::arc::Arc"
            )
        {
            return None;
        }
        if let ExprKind::MethodCall(segment, _, [], _) = expression.kind
            && copies_value(segment.ident.name.as_str())
        {
            return Some(segment.ident.name.to_string());
        }
        let mut context = expression.span.ctxt();
        while !context.is_root() {
            let data = context.outer_expn_data();
            if let rustc_span::hygiene::ExpnKind::Macro(_, name) = data.kind
                && name.as_str() == "format"
            {
                return Some("format!".into());
            }
            context = data.call_site.ctxt();
        }
        None
    }

    /// A copy whose only use is a borrow: `&value.clone()` or `value.to_string().len()`.
    fn borrowed_copy(&mut self, expression: &'tcx Expr<'tcx>, name: &str) {
        if !copies_value(name) || name == "into" {
            return;
        }
        if self.allocation(expression).is_none() {
            return;
        }
        let borrowed = match self.cx.tcx.parent_hir_node(expression.hir_id) {
            hir::Node::Expr(parent) => match parent.kind {
                ExprKind::AddrOf(hir::BorrowKind::Ref, _, _) => !self.keyed_argument(parent.hir_id),
                ExprKind::MethodCall(_, receiver, _, _) if receiver.hir_id == expression.hir_id => {
                    self.cx
                        .typeck_results()
                        .expr_adjustments(expression)
                        .iter()
                        .any(|adjustment| matches!(adjustment.kind, Adjust::Borrow(_)))
                }
                _ => false,
            },
            _ => false,
        };
        if !borrowed {
            return;
        }
        self.observations.push(Observation {
            owner: self.owner,
            span: expression.span,
            pattern: Pattern::BorrowedCopy {
                operation: name.to_owned(),
                value_type: self.cx.typeck_results().expr_ty(expression).to_string(),
            },
        });
    }

    /// The borrowed key argument of a keyed lookup, which reports its own allocation.
    fn keyed_argument(&self, node: HirId) -> bool {
        let hir::Node::Expr(parent) = self.cx.tcx.parent_hir_node(node) else {
            return false;
        };
        let Some(call) = self.call(parent) else {
            return false;
        };
        let Some(collection) = self.owner_path(call.definition) else {
            return false;
        };
        keyed_access(&collection, &call.name).is_some()
            && call.arguments.first().is_some_and(|key| key.hir_id == node)
    }

    fn error_type(&self, result: Ty<'tcx>) -> Option<Ty<'tcx>> {
        let mut result = self
            .cx
            .tcx
            .normalize_erasing_regions(self.cx.typing_env(), ty::Unnormalized::new_wip(result));
        while let ty::Ref(_, referent, _) = result.kind() {
            result = *referent;
        }
        let ty::Adt(adt, arguments) = result.kind() else {
            return None;
        };
        if !self
            .cx
            .tcx
            .is_diagnostic_item(Symbol::intern("Result"), adt.did())
        {
            return None;
        }
        Some(arguments.type_at(1))
    }

    /// What a failure value carries; zero-sized and uninhabited failures carry nothing.
    fn information(&self, error: Ty<'tcx>) -> Option<ErrorInformation> {
        if error.is_unit() || error.is_never() {
            return None;
        }
        if let ty::Adt(adt, _) = error.kind() {
            if adt.is_enum() && adt.variants().is_empty() {
                return None;
            }
            if self.cx.tcx.crate_name(adt.did().krate).as_str() == "error_stack"
                && self.cx.tcx.item_name(adt.did()).as_str() == "Report"
            {
                return Some(ErrorInformation::Report);
            }
        }
        if error.has_param() {
            return Some(ErrorInformation::Generic);
        }
        match self.cx.layout_of(error) {
            Ok(layout) if layout.is_zst() => None,
            Ok(_) => Some(ErrorInformation::Value),
            Err(_) => Some(ErrorInformation::Generic),
        }
    }

    fn discarded(&mut self, span: Span, operation: String, result: Ty<'tcx>) {
        let Some(error) = self.error_type(result) else {
            return;
        };
        let Some(information) = self.information(error) else {
            return;
        };
        self.observations.push(Observation {
            owner: self.owner,
            span,
            pattern: Pattern::DiscardedError {
                operation,
                error_type: error.to_string(),
                information,
            },
        });
    }

    /// A closure argument that never reads its first parameter.
    fn ignores_argument(&self, argument: &'tcx Expr<'tcx>) -> bool {
        let ExprKind::Closure(closure) = argument.kind else {
            return false;
        };
        let body = self.cx.tcx.hir_body(closure.body);
        let Some(parameter) = body.params.first() else {
            return false;
        };
        let binding = match parameter.pat.kind {
            PatKind::Wild => return true,
            PatKind::Binding(_, binding, _, None) => binding,
            _ => return false,
        };
        struct Uses {
            binding: HirId,
            found: bool,
        }
        impl<'v> Visitor<'v> for Uses {
            fn visit_expr(&mut self, expression: &'v Expr<'v>) {
                if let ExprKind::Path(QPath::Resolved(None, path)) = expression.kind
                    && path.res == Res::Local(self.binding)
                {
                    self.found = true;
                }
                intravisit::walk_expr(self, expression);
            }
        }
        let mut uses = Uses {
            binding,
            found: false,
        };
        uses.visit_expr(body.value);
        !uses.found
    }

    fn result_method(&mut self, call: &Call<'tcx>) {
        if !self.diagnostic_owner(call.definition, "Result") {
            return;
        }
        let discards = match call.name.as_str() {
            "ok" | "is_ok" | "is_err" | "unwrap_or" | "unwrap_or_default" => true,
            "unwrap_or_else" | "map_err" | "or_else" => call
                .arguments
                .first()
                .is_some_and(|argument| self.ignores_argument(argument)),
            _ => false,
        };
        if discards {
            let result = self.cx.typeck_results().expr_ty(call.receiver);
            self.discarded(call.span, format!("Result::{}", call.name), result);
        }
    }

    /// `checked_sub(..).unwrap_or(0)` or `u32::try_from(..).unwrap_or(u32::MAX)`: a clamp
    /// spelled as checked arithmetic or conversion. The more specific clamp replaces the
    /// conversion failure the same fallback drops.
    fn disguised_clamp(&mut self, call: &Call<'tcx>) -> bool {
        if !fallback(&call.name) {
            return false;
        }
        let mut source = call.receiver;
        if let Some(conversion) = self.call(source)
            && conversion.name == "ok"
            && self.diagnostic_owner(conversion.definition, "Result")
        {
            source = conversion.receiver;
        }
        let Some(operation) = self.call(source) else {
            return false;
        };
        let checked =
            checked_operation(&operation.name) && self.primitive_owner(operation.definition);
        let conversion = matches!(operation.name.as_str(), "try_from" | "try_into")
            && self.integral(self.cx.typeck_results().expr_ty(source));
        if !checked && !conversion {
            return false;
        }
        let fallback = match call.arguments.first() {
            Some(argument) => self.snippet(argument.span).unwrap_or_default(),
            None => "Default::default()".into(),
        };
        self.observations.push(Observation {
            owner: self.owner,
            span: call.span,
            pattern: Pattern::DisguisedClamp {
                operation: operation.name,
                fallback,
            },
        });
        true
    }

    fn primitive_owner(&self, definition: DefId) -> bool {
        let Some(implementation) = self.cx.tcx.inherent_impl_of_assoc(definition) else {
            return false;
        };
        let owner = self
            .cx
            .tcx
            .type_of(implementation)
            .instantiate_identity()
            .skip_norm_wip();
        if owner.is_integral() {
            return true;
        }
        self.adt_path(owner).as_deref() == Some("core::time::Duration")
    }

    /// An integer or a Result/Option of one, as a checked conversion returns.
    fn integral(&self, value: Ty<'tcx>) -> bool {
        if value.is_integral() {
            return true;
        }
        let ty::Adt(_, arguments) = value.kind() else {
            return false;
        };
        arguments
            .types()
            .next()
            .is_some_and(|inner| inner.is_integral())
    }

    fn result_pattern(&self, pattern: &'tcx hir::Pat<'tcx>, variant: LangItem) -> bool {
        let PatKind::TupleStruct(ref path, _, _) = pattern.kind else {
            return false;
        };
        let Res::Def(DefKind::Ctor(..), constructor) = self.cx.qpath_res(path, pattern.hir_id)
        else {
            return false;
        };
        let Some(expected) = self.cx.tcx.lang_items().get(variant) else {
            return false;
        };
        self.cx.tcx.opt_parent(constructor) == Some(expected)
    }

    /// `Err(_)` keeps none of the failure; `Err(error)` hands it to the arm.
    fn ignores_error(&self, pattern: &'tcx hir::Pat<'tcx>) -> bool {
        if !self.result_pattern(pattern, LangItem::ResultErr) {
            return false;
        }
        let PatKind::TupleStruct(_, fields, _) = pattern.kind else {
            return false;
        };
        fields
            .iter()
            .all(|field| matches!(field.kind, PatKind::Wild))
    }

    fn statements(&self, branch: &'tcx Expr<'tcx>) -> Vec<Statement> {
        let mut result = Vec::new();
        let ExprKind::Block(block, _) = branch.kind else {
            if let Some(text) = self.statement(branch) {
                result.push(Statement {
                    text,
                    span: branch.span,
                });
            }
            return result;
        };
        for statement in block.stmts {
            let expression = match statement.kind {
                StmtKind::Semi(expression) | StmtKind::Expr(expression) => expression,
                StmtKind::Let(local) => {
                    if let Some(init) = local.init
                        && matches!(init.kind, ExprKind::Call(..) | ExprKind::MethodCall(..))
                        && let Some(text) = self.snippet(statement.span)
                    {
                        result.push(Statement {
                            text: normalized(&text),
                            span: statement.span,
                        });
                    }
                    continue;
                }
                StmtKind::Item(_) => continue,
            };
            if let Some(text) = self.statement(expression) {
                result.push(Statement {
                    text,
                    span: statement.span,
                });
            }
        }
        if let Some(tail) = block.expr
            && let Some(text) = self.statement(tail)
        {
            result.push(Statement {
                text,
                span: tail.span,
            });
        }
        result
    }

    /// Calls and assignments are work worth hoisting; exits and values are not.
    fn statement(&self, expression: &'tcx Expr<'tcx>) -> Option<String> {
        if expression.span.from_expansion() {
            return None;
        }
        if !matches!(
            expression.kind,
            ExprKind::Call(..)
                | ExprKind::MethodCall(..)
                | ExprKind::Assign(..)
                | ExprKind::AssignOp(..)
        ) {
            return None;
        }
        if self.cx.typeck_results().expr_ty(expression).is_never() {
            return None;
        }
        Some(normalized(&self.snippet(expression.span)?))
    }

    fn duplicated(&mut self, construct: Construct, branches: &[&'tcx Expr<'tcx>]) {
        if branches.len() < 2 {
            return;
        }
        let statements: Vec<Vec<Statement>> = branches
            .iter()
            .map(|branch| self.statements(branch))
            .collect();
        let mut reported = HashSet::new();
        for first in &statements[0] {
            let text = &first.text;
            if !reported.insert(text.clone()) {
                continue;
            }
            let mut occurrences = Vec::new();
            for branch in &statements {
                let Some(statement) = branch.iter().find(|candidate| &candidate.text == text)
                else {
                    break;
                };
                occurrences.push(statement.span);
            }
            if occurrences.len() != statements.len() {
                continue;
            }
            self.observations.push(Observation {
                owner: self.owner,
                span: occurrences[0],
                pattern: Pattern::DuplicatedBranch {
                    construct,
                    statement: text.clone(),
                    occurrences,
                },
            });
        }
    }

    /// The branches of an exhaustive `if` chain; a chain without a final `else` has none.
    fn chain(&self, mut expression: &'tcx Expr<'tcx>) -> Vec<&'tcx Expr<'tcx>> {
        let mut branches = Vec::new();
        loop {
            let ExprKind::If(_, then, otherwise) = expression.kind else {
                branches.push(expression);
                return branches;
            };
            branches.push(then);
            let Some(otherwise) = otherwise else {
                return Vec::new();
            };
            expression = otherwise;
        }
    }

    fn visit_region(&mut self, conditional: HirId, region: Region, expression: &'tcx Expr<'tcx>) {
        self.steps.push(Step {
            conditional,
            region,
        });
        self.visit_expr(expression);
        self.steps.pop();
    }

    fn repeated_lookups(&mut self) {
        let mut groups: BTreeMap<Group, Vec<usize>> = BTreeMap::new();
        for (index, operation) in self.keyed.iter().enumerate() {
            groups
                .entry(Group {
                    receiver: operation.receiver.key.clone(),
                    key: operation.key.key.clone(),
                })
                .or_default()
                .push(index);
        }
        for members in groups.into_values() {
            if members.len() < 2 {
                continue;
            }
            let mut ordered = members;
            ordered.sort_by_key(|index| self.keyed[*index].span.lo());
            let mut related = Vec::new();
            let mut guarded = false;
            for (position, earlier) in ordered.iter().enumerate() {
                for later in &ordered[position + 1..] {
                    let first = &self.keyed[*earlier];
                    let second = &self.keyed[*later];
                    if exclusive(&first.steps, &second.steps) || self.changed_between(first, second)
                    {
                        continue;
                    }
                    guarded |= guards(&first.steps, &second.steps);
                    related.push(*earlier);
                    related.push(*later);
                }
            }
            related.sort_by_key(|index| self.keyed[*index].span.lo());
            related.dedup();
            if related.len() < 2 {
                continue;
            }
            let first = &self.keyed[related[0]];
            let observation = Observation {
                owner: self.owner,
                span: first.span,
                pattern: Pattern::RepeatedLookup {
                    collection: first.collection.clone(),
                    receiver: first.receiver.display.clone(),
                    key: first.key.display.clone(),
                    guarded,
                    operations: related
                        .iter()
                        .map(|index| Operation {
                            name: self.keyed[*index].operation.clone(),
                            span: self.keyed[*index].span,
                        })
                        .collect(),
                },
            };
            self.observations.push(observation);
        }
    }

    /// An assignment between two operations can change their receiver or key.
    fn changed_between(&self, first: &Keyed, second: &Keyed) -> bool {
        for assignment in &self.assignments {
            if assignment.span.lo() < first.span.hi() || assignment.span.lo() >= second.span.lo() {
                continue;
            }
            if first.receiver.overlaps(&assignment.place) || first.key.overlaps(&assignment.place) {
                return true;
            }
        }
        false
    }
}

impl<'tcx> Visitor<'tcx> for Walker<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        match expression.kind {
            ExprKind::If(condition, then, otherwise) => {
                let nested = matches!(
                    self.cx.tcx.parent_hir_node(expression.hir_id),
                    hir::Node::Expr(Expr { kind: ExprKind::If(_, _, Some(parent)), .. })
                        if parent.hir_id == expression.hir_id
                );
                if !nested && !expression.span.from_expansion() {
                    let branches = self.chain(expression);
                    self.duplicated(Construct::If, &branches);
                }
                if let ExprKind::Let(binding) = condition.kind
                    && otherwise.is_none()
                    && self.result_pattern(binding.pat, LangItem::ResultOk)
                {
                    let result = self.cx.typeck_results().expr_ty(binding.init);
                    self.discarded(condition.span, "if let Ok".into(), result);
                }
                self.visit_region(expression.hir_id, Region::Condition, condition);
                self.visit_region(expression.hir_id, Region::Branch(0), then);
                if let Some(otherwise) = otherwise {
                    self.visit_region(expression.hir_id, Region::Branch(1), otherwise);
                }
                return;
            }
            ExprKind::Match(scrutinee, arms, MatchSource::Normal) => {
                if !expression.span.from_expansion() {
                    let branches: Vec<&'tcx Expr<'tcx>> = arms.iter().map(|arm| arm.body).collect();
                    self.duplicated(Construct::Match, &branches);
                }
                for arm in arms {
                    if self.ignores_error(arm.pat) {
                        let result = self.cx.typeck_results().expr_ty(scrutinee);
                        self.discarded(arm.pat.span, "match Err(_)".into(), result);
                    }
                }
                self.visit_region(expression.hir_id, Region::Condition, scrutinee);
                for (index, arm) in arms.iter().enumerate() {
                    self.steps.push(Step {
                        conditional: expression.hir_id,
                        region: Region::Branch(index),
                    });
                    self.visit_pat(arm.pat);
                    if let Some(guard) = arm.guard {
                        self.visit_expr(guard);
                    }
                    self.visit_expr(arm.body);
                    self.steps.pop();
                }
                return;
            }
            ExprKind::Assign(target, _, _) | ExprKind::AssignOp(_, target, _) => {
                self.assignment(target, expression.span);
            }
            ExprKind::Index(base, index, _) => {
                self.index(expression, base, index);
            }
            ExprKind::MethodCall(segment, ..) => {
                if let Some(call) = self.call(expression) {
                    self.keyed(&call);
                    if !self.disguised_clamp(&call) {
                        self.result_method(&call);
                    }
                }
                self.borrowed_copy(expression, segment.ident.name.as_str());
            }
            ExprKind::Call(..) => {
                if let Some(call) = self.call(expression) {
                    self.keyed(&call);
                    if !self.disguised_clamp(&call) {
                        self.result_method(&call);
                    }
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expression);
    }

    fn visit_local(&mut self, local: &'tcx hir::LetStmt<'tcx>) {
        let (Some(init), Some(otherwise)) = (local.init, local.els) else {
            intravisit::walk_local(self, local);
            return;
        };
        if self.result_pattern(local.pat, LangItem::ResultOk) {
            let result = self.cx.typeck_results().expr_ty(init);
            self.discarded(local.pat.span, "let Ok else".into(), result);
        }
        self.visit_region(local.hir_id, Region::Condition, init);
        self.visit_pat(local.pat);
        self.steps.push(Step {
            conditional: local.hir_id,
            region: Region::Branch(1),
        });
        self.visit_block(otherwise);
        self.steps.pop();
    }
}

/// A function whose whole body passes its parameters, in order, to one local call.
fn forwarding_wrapper<'tcx>(
    cx: &LateContext<'tcx>,
    owner: LocalDefId,
    body: &hir::Body<'tcx>,
) -> Option<Observation> {
    if !matches!(cx.tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn) {
        return None;
    }
    let mut expression = body.value;
    while let ExprKind::Block(block, _) = expression.kind
        && block.stmts.is_empty()
        && let Some(tail) = block.expr
    {
        expression = tail;
    }
    if expression.span.from_expansion() {
        return None;
    }
    let typeck = cx.typeck_results();
    let (callee, arguments): (DefId, Vec<&Expr<'_>>) = match expression.kind {
        ExprKind::MethodCall(_, receiver, arguments, _) => {
            let callee = typeck.type_dependent_def_id(expression.hir_id)?;
            let mut all = vec![receiver];
            all.extend(arguments.iter());
            (callee, all)
        }
        ExprKind::Call(function, arguments) => {
            let ty::FnDef(callee, _) = *typeck.expr_ty(function).kind() else {
                return None;
            };
            (callee, arguments.iter().collect())
        }
        _ => return None,
    };
    if !callee.is_local() || body.params.is_empty() || arguments.len() != body.params.len() {
        return None;
    }
    for (parameter, argument) in body.params.iter().zip(arguments) {
        let PatKind::Binding(_, binding, _, None) = parameter.pat.kind else {
            return None;
        };
        let mut argument = argument;
        while let ExprKind::AddrOf(_, _, inner) | ExprKind::Unary(UnOp::Deref, inner) =
            argument.kind
        {
            argument = inner;
        }
        let ExprKind::Path(QPath::Resolved(None, path)) = argument.kind else {
            return None;
        };
        if path.res != Res::Local(binding) {
            return None;
        }
    }
    let contract = has_contract(cx, owner.to_def_id()) || has_contract(cx, callee);
    let mut implementation = false;
    if let Some(item) = cx.tcx.opt_associated_item(owner.to_def_id()) {
        implementation = item.trait_item_def_id().is_some();
    }
    let map = cx.tcx.sess.source_map();
    let wrapper_file = map
        .lookup_char_pos(cx.tcx.def_span(owner).lo())
        .file
        .name
        .clone();
    let callee_file = map
        .lookup_char_pos(cx.tcx.def_span(callee).lo())
        .file
        .name
        .clone();
    Some(Observation {
        owner,
        span: cx.tcx.def_span(owner),
        pattern: Pattern::ForwardingWrapper {
            callee,
            contract,
            implementation,
            relocated: wrapper_file != callee_file,
        },
    })
}

fn has_contract(cx: &LateContext<'_>, definition: DefId) -> bool {
    ["context", "dispatch"].iter().any(|name| {
        cx.tcx
            .get_attrs_by_path(
                definition,
                &[Symbol::intern("nervix"), Symbol::intern(name)],
            )
            .next()
            .is_some()
    })
}
