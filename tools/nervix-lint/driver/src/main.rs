//! Repository tooling, outside the product layer order.
//! Owns: the pinned compiler callback and resolved synchronization acquisition pass.
//! Depends on: rustc internals, the report vocabulary and the primitive boundary.
//! Must not know: manually maintained answers about product source or runtime graph execution.

#![feature(rustc_private)]

extern crate rustc_ast;
extern crate rustc_attr_ir;
extern crate rustc_data_structures;
extern crate rustc_driver;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_interface;
extern crate rustc_lint;
extern crate rustc_lint_defs;
extern crate rustc_middle;
extern crate rustc_session;
extern crate rustc_span;

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context as _, bail};
use meticulous::ResultExt as _;
use nervix_lint_report::{CompilerReport, Expansion, Finding, SiteId, SourceSpan, rules};
use nervix_primitives::sync::blocking::Mutex;
use rustc_data_structures::marker::IntoDynSyncSend;
use rustc_driver::{Callbacks, Compilation};
use rustc_hir::{Expr, ExprKind};
use rustc_interface::interface;
use rustc_lint::{LateContext, LateLintPass};
use rustc_lint_defs::impl_lint_pass;

mod contracts;
use contracts::{Flow, INVALID_CONTRACT, LIFECYCLE_CALL, SYNC_ACQUISITION, UNKNOWN_EFFECT};
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::Span;
use triomphe::Arc;

struct AcquisitionPass {
    flow: Flow,
    authored_files: BTreeMap<String, bool>,
    root: PathBuf,
    report: Arc<Mutex<CompilerReport>>,
}

impl_lint_pass!(AcquisitionPass => [SYNC_ACQUISITION, LIFECYCLE_CALL, UNKNOWN_EFFECT, INVALID_CONTRACT]);

struct PassInputs {
    root: PathBuf,
    report: Arc<Mutex<CompilerReport>>,
}

struct ResolvedCall<'tcx> {
    definition: rustc_span::def_id::DefId,
    arguments: ty::GenericArgsRef<'tcx>,
    receiver: Ty<'tcx>,
    receiver_expression: String,
    span: Span,
}

impl AcquisitionPass {
    fn authored(&mut self, cx: &LateContext<'_>, span: Span) -> bool {
        let file = cx
            .tcx
            .sess
            .source_map()
            .lookup_char_pos(span.lo())
            .file
            .name
            .prefer_local_unconditionally()
            .to_string();
        *self.authored_files.entry(file.clone()).or_insert_with(|| {
            let Ok(path) = Path::new(&file).canonicalize() else {
                return false;
            };
            let Ok(relative) = path.strip_prefix(&self.root) else {
                return false;
            };
            let path = relative.to_string_lossy();
            !path.starts_with("target/") && !path.contains("/target/")
        })
    }
    fn definition_path(&self, cx: &LateContext<'_>, id: rustc_span::def_id::DefId) -> String {
        format!(
            "{}{}",
            cx.tcx.crate_name(id.krate),
            cx.tcx.def_path(id).to_string_no_crate_verbose()
        )
    }

    fn source_span(&self, cx: &LateContext<'_>, span: Span) -> Option<SourceSpan> {
        let map = cx.tcx.sess.source_map();
        let lo = map.lookup_char_pos(span.lo());
        let hi = map.lookup_char_pos(span.hi());
        let file = lo.file.name.prefer_local_unconditionally().to_string();
        let absolute = match Path::new(&file).canonicalize() {
            Ok(path) => path,
            Err(_) => {
                self.report.lock().excluded_generated += 1;
                return None;
            }
        };
        let relative = match absolute.strip_prefix(&self.root) {
            Ok(path) => path,
            Err(_) => {
                self.report.lock().excluded_external += 1;
                return None;
            }
        };
        let path = relative.to_string_lossy().replace('\\', "/");
        if path.starts_with("target/") || path.contains("/target/") {
            self.report.lock().excluded_generated += 1;
            return None;
        }
        let source = match map.span_to_snippet(span) {
            Ok(source) => source,
            Err(_) => {
                self.report.lock().excluded_generated += 1;
                return None;
            }
        };
        Some(SourceSpan {
            site: SiteId {
                path,
                start: (span.lo() - lo.file.start_pos).0,
            },
            end: (span.hi() - lo.file.start_pos).0,
            line: u32::try_from(lo.line).assured("a source file's line offsets fit in BytePos"),
            column: u32::try_from(lo.col.0).assured("a source column fits in BytePos"),
            end_line: u32::try_from(hi.line).assured("a source file's line offsets fit in BytePos"),
            end_column: u32::try_from(hi.col.0).assured("a source column fits in BytePos"),
            source,
        })
    }

    fn expansions(&self, cx: &LateContext<'_>, span: Span) -> Vec<Expansion> {
        let mut result = Vec::new();
        let mut context = span.ctxt();
        while !context.is_root() {
            let data = context.outer_expn_data();
            result.push(Expansion {
                macro_name: format!("{:?}", data.kind),
                call_site: cx
                    .tcx
                    .sess
                    .source_map()
                    .span_to_diagnostic_string(data.call_site),
                definition: Some(
                    cx.tcx
                        .sess
                        .source_map()
                        .span_to_diagnostic_string(data.def_site),
                ),
            });
            context = data.call_site.ctxt();
        }
        result
    }

    fn receiver<'tcx>(&self, cx: &LateContext<'tcx>, ty: Ty<'tcx>) -> Option<(String, String)> {
        let mut normalized = cx
            .tcx
            .normalize_erasing_regions(cx.typing_env(), ty::Unnormalized::new_wip(ty));
        let display = normalized.to_string();
        loop {
            match normalized.kind() {
                ty::Ref(_, referent, _) => normalized = *referent,
                ty::Adt(adt, arguments) => {
                    let name = self.definition_path(cx, adt.did());
                    if matches!(
                        name.as_str(),
                        "alloc::sync::Arc"
                            | "triomphe::arc::Arc"
                            | "alloc::boxed::Box"
                            | "core::pin::Pin"
                    ) {
                        normalized = arguments.type_at(0);
                    } else {
                        return Some((name, display));
                    }
                }
                _ => return None,
            }
        }
    }
}

impl<'tcx> LateLintPass<'tcx> for AcquisitionPass {
    fn check_crate(&mut self, cx: &LateContext<'tcx>) {
        self.flow.validate(cx, rustc_hir::CRATE_HIR_ID, true);
    }
    fn check_crate_post(&mut self, cx: &LateContext<'tcx>) {
        let mut report = self.report.lock();
        self.flow.finish(cx, &mut report.findings);
        report.complete = true;
    }
    fn check_body(&mut self, cx: &LateContext<'tcx>, body: &rustc_hir::Body<'tcx>) {
        self.flow.body(cx, body);
    }
    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx rustc_hir::Item<'tcx>) {
        self.flow.validate(
            cx,
            item.hir_id(),
            matches!(
                item.kind,
                rustc_hir::ItemKind::Fn { .. }
                    | rustc_hir::ItemKind::Mod(..)
                    | rustc_hir::ItemKind::Struct(..)
                    | rustc_hir::ItemKind::Enum(..)
                    | rustc_hir::ItemKind::Union(..)
                    | rustc_hir::ItemKind::Trait { .. }
                    | rustc_hir::ItemKind::Impl(..)
            ),
        );
    }
    fn check_impl_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx rustc_hir::ImplItem<'tcx>) {
        self.flow.validate(
            cx,
            item.hir_id(),
            matches!(item.kind, rustc_hir::ImplItemKind::Fn(..)),
        );
    }
    fn check_trait_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx rustc_hir::TraitItem<'tcx>) {
        self.flow.validate(
            cx,
            item.hir_id(),
            matches!(item.kind, rustc_hir::TraitItemKind::Fn(..)),
        );
    }
    fn check_field_def(&mut self, cx: &LateContext<'tcx>, field: &'tcx rustc_hir::FieldDef<'tcx>) {
        self.flow.validate(cx, field.hir_id, false);
    }
    fn check_generic_param(
        &mut self,
        cx: &LateContext<'tcx>,
        parameter: &'tcx rustc_hir::GenericParam<'tcx>,
    ) {
        self.flow.validate(cx, parameter.hir_id, false);
    }
    fn check_variant(&mut self, cx: &LateContext<'tcx>, variant: &'tcx rustc_hir::Variant<'tcx>) {
        self.flow.validate(cx, variant.hir_id, false);
    }
    fn check_local(&mut self, cx: &LateContext<'tcx>, local: &'tcx rustc_hir::LetStmt<'tcx>) {
        self.flow.validate(cx, local.hir_id, false);
    }
    fn check_stmt(&mut self, cx: &LateContext<'tcx>, statement: &'tcx rustc_hir::Stmt<'tcx>) {
        self.flow.validate(cx, statement.hir_id, false);
    }

    fn check_expr(&mut self, cx: &LateContext<'tcx>, expression: &'tcx Expr<'tcx>) {
        self.flow.validate(
            cx,
            expression.hir_id,
            matches!(expression.kind, ExprKind::Closure(..)),
        );
        if let ExprKind::Closure(closure) = expression.kind {
            let authored = self.authored(cx, expression.span);
            self.flow.call(
                cx,
                expression,
                Some(closure.def_id.to_def_id()),
                false,
                authored,
            );
        }
        // Local edges include language lowering, so user-defined iteration and polling bodies
        // are reconsidered. Authored macro tokens retain their own source span.
        let typeck = cx.typeck_results();
        let mut call = match expression.kind {
            ExprKind::MethodCall(segment, receiver, _, _) => {
                let Some(definition) = typeck.type_dependent_def_id(expression.hir_id) else {
                    let authored = self.authored(cx, segment.ident.span);
                    self.flow.call(cx, expression, None, true, authored);
                    return;
                };
                let selected = ty::Instance::try_resolve(
                    cx.tcx,
                    cx.typing_env().with_post_analysis_normalized(cx.tcx),
                    definition,
                    typeck.node_args(expression.hir_id),
                )
                .ok()
                .flatten();
                let indirect = cx.tcx.trait_of_assoc(definition).is_some()
                    && selected.is_none_or(|instance| {
                        matches!(instance.def, ty::InstanceKind::Virtual(..))
                    })
                    && !contracts::has_callee_contract(cx.tcx, definition);
                let target = match selected {
                    Some(instance) => instance.def_id(),
                    None => definition,
                };
                let authored = self.authored(cx, segment.ident.span);
                self.flow
                    .call(cx, expression, Some(target), indirect, authored);
                ResolvedCall {
                    definition,
                    arguments: typeck.node_args(expression.hir_id),
                    receiver: typeck.expr_ty_adjusted(receiver),
                    receiver_expression: cx
                        .tcx
                        .sess
                        .source_map()
                        .span_to_snippet(receiver.span)
                        .unwrap_or_else(|_| "compiler-generated receiver".into()),
                    span: segment.ident.span,
                }
            }
            ExprKind::Call(function, arguments) => {
                let ty::FnDef(definition, substitutions) = *typeck.expr_ty(function).kind() else {
                    if let ty::Closure(definition, _) = *typeck.expr_ty(function).kind() {
                        let authored = self.authored(cx, function.span);
                        self.flow
                            .call(cx, expression, Some(definition), false, authored);
                    } else {
                        let authored = self.authored(cx, function.span);
                        self.flow.call(cx, expression, None, true, authored);
                    }
                    return;
                };
                let selected = ty::Instance::try_resolve(
                    cx.tcx,
                    cx.typing_env().with_post_analysis_normalized(cx.tcx),
                    definition,
                    substitutions.skip_binder(),
                )
                .ok()
                .flatten();
                let target = match selected {
                    Some(instance) => instance.def_id(),
                    None => definition,
                };
                let indirect = cx.tcx.trait_of_assoc(definition).is_some()
                    && selected.is_none_or(|instance| {
                        matches!(instance.def, ty::InstanceKind::Virtual(..))
                    })
                    && !contracts::has_callee_contract(cx.tcx, definition);
                let authored = self.authored(cx, function.span);
                self.flow
                    .call(cx, expression, Some(target), indirect, authored);
                let Some(receiver) = arguments.first() else {
                    return;
                };
                ResolvedCall {
                    definition,
                    arguments: substitutions.skip_binder(),
                    receiver: typeck.expr_ty_adjusted(receiver),
                    receiver_expression: cx
                        .tcx
                        .sess
                        .source_map()
                        .span_to_snippet(receiver.span)
                        .unwrap_or_else(|_| "compiler-generated receiver".into()),
                    span: function.span,
                }
            }
            _ => return,
        };
        let operation = cx.tcx.item_name(call.definition).to_string();
        if !rules::may_acquire(&operation) {
            return;
        }
        if cx.tcx.trait_of_assoc(call.definition).is_some()
            && let Ok(Some(instance)) =
                ty::Instance::try_resolve(cx.tcx, cx.typing_env(), call.definition, call.arguments)
        {
            call.definition = instance.def_id();
            call.arguments = instance.args;
        }
        // The instantiated formal receiver is authoritative for aliases, UFCS and owned-lock APIs.
        let signature = cx
            .tcx
            .fn_sig(call.definition)
            .instantiate(cx.tcx, call.arguments)
            .skip_norm_wip()
            .skip_binder();
        let receiver = match signature.inputs().first() {
            Some(formal) => *formal,
            None => call.receiver,
        };
        // IntoIterator on an owned DashMap consumes shard storage. Its borrowed implementation
        // creates a lazy read iterator, so only the borrowed operation acquires shard guards.
        if operation == "into_iter" && !matches!(receiver.kind(), ty::Ref(..)) {
            return;
        }
        let Some((receiver_name, receiver_type)) = self.receiver(cx, receiver) else {
            return;
        };
        let defining_crate = cx.tcx.crate_name(call.definition.krate).to_string();
        let Some(acquisition) = rules::acquisition(&receiver_name, &defining_crate, &operation)
        else {
            return;
        };
        let expansion = self.expansions(cx, call.span);
        // Keep authored tokens even when an external macro supplied their expansion context.
        let Some(source) = self.source_span(cx, call.span) else {
            return;
        };
        let owner = cx.tcx.hir_enclosing_body_owner(expression.hir_id);
        let mut report = self.report.lock();
        self.flow
            .acquisition(cx, expression, expression.span, report.findings.len());
        report.findings.push(Finding {
            rule: "data_plane_lock_acquisitions".into(),
            receiver: receiver_name,
            receiver_type,
            receiver_expression: call.receiver_expression,
            operation,
            definition: self.definition_path(cx, call.definition),
            owner: self.definition_path(cx, owner.to_def_id()),
            acquisition,
            context: None,
            span: source,
            expansion,
        });
    }
}

struct Analysis {
    root: PathBuf,
    destination: PathBuf,
    report: Arc<Mutex<CompilerReport>>,
    failure: Option<std::io::Error>,
}

impl Callbacks for Analysis {
    fn config(&mut self, configuration: &mut interface::Config) {
        let root = self.root.clone();
        let report = self.report.clone();
        let suppressed = configuration.opts.lint_cap.is_some()
            || configuration.opts.lint_opts.iter().any(|(name, level)| {
                *level == rustc_lint_defs::Level::Allow
                    && (name.starts_with("nervix::") || name == "warnings")
            });
        configuration.register_lints = Some(Box::new(move |session, store| {
            if suppressed {
                session.dcx().fatal(
                    "Nervix analysis rejects lint caps and blanket allow levels; use a \
                     reason-bearing expectation on one operation",
                );
            }
            store.register_lints(&[
                SYNC_ACQUISITION,
                LIFECYCLE_CALL,
                UNKNOWN_EFFECT,
                INVALID_CONTRACT,
            ]);
            let inputs = IntoDynSyncSend(PassInputs {
                root: root.clone(),
                report: report.clone(),
            });
            store.register_late_lint_pass(Box::new(move |_| {
                let inputs = &*inputs;
                Box::new(AcquisitionPass {
                    flow: Flow::default(),
                    authored_files: BTreeMap::new(),
                    root: inputs.root.clone(),
                    report: inputs.report.clone(),
                })
            }));
        }));
    }

    fn after_analysis<'tcx>(
        &mut self,
        _compiler: &interface::Compiler,
        _tcx: TyCtxt<'tcx>,
    ) -> Compilation {
        let report = self.report.lock();
        if !report.complete {
            self.failure = Some(std::io::Error::other(
                "the acquisition lint pass did not complete",
            ));
            return Compilation::Stop;
        }
        let bytes = serde_json::to_vec_pretty(&*report)
            .assured("compiler reports contain only JSON-compatible values");
        let temporary = self
            .destination
            .with_extension(format!("{}.tmp", std::process::id()));
        if let Err(error) = fs::write(&temporary, bytes) {
            self.failure = Some(error);
            return Compilation::Stop;
        }
        if let Err(error) = fs::rename(&temporary, &self.destination) {
            self.failure = Some(error);
            return Compilation::Stop;
        }
        Compilation::Continue
    }
}

fn main() -> anyhow::Result<()> {
    let mut arguments: Vec<String> = env::args().collect();
    // Cargo nests this driver below its configured RUSTC_WRAPPER and supplies the real rustc.
    if arguments
        .get(1)
        .is_some_and(|argument| !argument.starts_with('-'))
    {
        arguments.remove(0);
    } else {
        let path = Command::new("rustup")
            .args(["which", "--toolchain", "nightly-2026-09-17", "rustc"])
            .output()?;
        if !path.status.success() {
            bail!("cannot locate the pinned compiler");
        }
        arguments[0] = String::from_utf8(path.stdout)?.trim().to_owned();
    }
    let compiler = arguments.first().context("missing rustc executable")?;
    let input = arguments.iter().find(|argument| argument.ends_with(".rs"));
    if input.is_none() || !arguments.iter().any(|argument| argument == "--crate-name") {
        let status = Command::new(compiler).args(&arguments[1..]).status()?;
        std::process::exit(status.code().unwrap_or(1));
    }
    let source = Path::new(input.context("the compilation invocation has a source input")?)
        .canonicalize()?
        .to_string_lossy()
        .into_owned();
    let crate_name_index = arguments
        .iter()
        .position(|argument| argument == "--crate-name")
        .context("missing crate name")?;
    let crate_name = arguments
        .get(crate_name_index + 1)
        .context("missing crate name value")?
        .clone();
    let root = PathBuf::from(
        env::var("NERVIX_LINT_ROOT").context("analysis driver requires NERVIX_LINT_ROOT")?,
    )
    .canonicalize()?;
    let identity =
        env::var("NERVIX_LINT_IDENTITY").context("analysis driver requires its report identity")?;
    let configuration = env::var("NERVIX_LINT_CONFIGURATION")
        .context("analysis driver requires its declared configuration")?;
    let version = Command::new(compiler).arg("-vV").output()?;
    if !version.status.success() {
        bail!("cannot obtain the compiler identity");
    }
    let compiler_identity = String::from_utf8(version.stdout)?;
    if !compiler_identity.contains("commit-hash: 923c95cdf") {
        bail!("the analysis compiler does not match nightly-2026-09-17");
    }
    let extra_filename = arguments
        .windows(2)
        .find(|pair| pair[0] == "-C" && pair[1].starts_with("extra-filename="));
    let suffix = match extra_filename {
        Some(pair) => pair[1].trim_start_matches("extra-filename="),
        None => "",
    };
    let destination = PathBuf::from(
        env::var("NERVIX_LINT_REPORTS").context("analysis driver requires its report directory")?,
    )
    .join(format!("{crate_name}{suffix}.json"));
    arguments.extend(
        [
            "--cfg=nervix_lint",
            "--check-cfg=cfg(nervix_lint)",
            "-Zcrate-attr=feature(register_tool,custom_inner_attributes)",
            "-Zcrate-attr=register_tool(nervix)",
            "-Funfulfilled_lint_expectations",
            "-Fnervix::invalid_contract",
        ]
        .map(String::from),
    );
    if env::var("NERVIX_LINT_MODE").as_deref() == Ok("inventory") {
        arguments.extend(
            [
                "-Wnervix::sync_acquisition",
                "-Wnervix::lifecycle_call",
                "-Wnervix::unknown_effect",
            ]
            .map(String::from),
        );
    }
    let report = CompilerReport {
        compiler: compiler_identity,
        identity,
        configuration,
        crate_name,
        crate_source: source,
        arguments: arguments.clone(),
        findings: Vec::new(),
        excluded_generated: 0,
        excluded_external: 0,
        complete: false,
    };
    let mut analysis = Analysis {
        root,
        destination,
        report: Arc::new(Mutex::new(report)),
        failure: None,
    };
    rustc_driver::run_compiler(&arguments, &mut analysis);
    if let Some(failure) = analysis.failure {
        return Err(failure).context("cannot publish the required compiler report");
    }
    Ok(())
}
