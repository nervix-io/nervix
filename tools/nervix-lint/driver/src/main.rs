//! Repository tooling, outside the product layer order.
//! Owns: the pinned compiler callback and resolved synchronization acquisition pass.
//! Depends on: rustc internals, the report vocabulary and the primitive boundary.
//! Must not know: runtime graph policy or whether an acquisition is justified.

#![feature(rustc_private)]

extern crate rustc_data_structures;
extern crate rustc_driver;
extern crate rustc_hir;
extern crate rustc_interface;
extern crate rustc_lint;
extern crate rustc_lint_defs;
extern crate rustc_middle;
extern crate rustc_session;
extern crate rustc_span;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context as _, bail};
use meticulous::ResultExt as _;
use nervix_lint_report::{Catalog, CompilerReport, Expansion, Finding, SiteId, SourceSpan};
use nervix_primitives::sync::blocking::Mutex;
use rustc_data_structures::marker::IntoDynSyncSend;
use rustc_driver::{Callbacks, Compilation};
use rustc_hir::{Expr, ExprKind};
use rustc_interface::interface;
use rustc_lint::{LateContext, LateLintPass};
use rustc_lint_defs::{declare_lint, impl_lint_pass};
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::Span;
use triomphe::Arc;

declare_lint! {
    pub NERVIX_SYNC_ACQUISITION,
    Warn,
    "inventory synchronization by its resolved API and adjusted receiver"
}

struct AcquisitionPass {
    catalog: Arc<Catalog>,
    root: PathBuf,
    report: Arc<Mutex<CompilerReport>>,
}

impl_lint_pass!(AcquisitionPass => [NERVIX_SYNC_ACQUISITION]);

struct PassInputs {
    catalog: Arc<Catalog>,
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
    fn check_crate_post(&mut self, _cx: &LateContext<'tcx>) {
        self.report.lock().complete = true;
    }

    fn check_expr(&mut self, cx: &LateContext<'tcx>, expression: &'tcx Expr<'tcx>) {
        let typeck = cx.typeck_results();
        let mut call = match expression.kind {
            ExprKind::MethodCall(segment, receiver, _, _) => {
                let Some(definition) = typeck.type_dependent_def_id(expression.hir_id) else {
                    return;
                };
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
                    return;
                };
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
        if !self
            .catalog
            .apis
            .iter()
            .any(|api| api.operations.contains_key(&operation))
        {
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
        let Some(acquisition) =
            self.catalog
                .acquisition(&receiver_name, &defining_crate, &operation)
        else {
            return;
        };
        let expansion = self.expansions(cx, call.span);
        // Keep authored tokens even when an external macro supplied their expansion context.
        let Some(source) = self.source_span(cx, call.span) else {
            return;
        };
        let owner = cx.tcx.hir_enclosing_body_owner(expression.hir_id);
        self.report.lock().findings.push(Finding {
            rule: "data_plane_lock_acquisitions".into(),
            receiver: receiver_name,
            receiver_type,
            receiver_expression: call.receiver_expression,
            operation,
            definition: self.definition_path(cx, call.definition),
            owner: self.definition_path(cx, owner.to_def_id()),
            acquisition,
            span: source,
            expansion,
        });
    }
}

struct Analysis {
    catalog: Arc<Catalog>,
    root: PathBuf,
    destination: PathBuf,
    report: Arc<Mutex<CompilerReport>>,
    failure: Option<std::io::Error>,
}

impl Callbacks for Analysis {
    fn config(&mut self, configuration: &mut interface::Config) {
        let catalog = self.catalog.clone();
        let root = self.root.clone();
        let report = self.report.clone();
        configuration.register_lints = Some(Box::new(move |_, store| {
            store.register_lints(&[NERVIX_SYNC_ACQUISITION]);
            let inputs = IntoDynSyncSend(PassInputs {
                catalog: catalog.clone(),
                root: root.clone(),
                report: report.clone(),
            });
            store.register_late_lint_pass(Box::new(move |_| {
                let inputs = &*inputs;
                Box::new(AcquisitionPass {
                    catalog: inputs.catalog.clone(),
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
    let catalog_path = root.join("tools/nervix-lint/catalog.json");
    let catalog =
        Catalog::parse(&fs::read(catalog_path)?).map_err(|error| anyhow::anyhow!("{error:?}"))?;
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
    arguments.push("--force-warn=nervix_sync_acquisition".into());
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
        catalog: Arc::new(catalog),
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
