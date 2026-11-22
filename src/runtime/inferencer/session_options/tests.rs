//! Owner tests: the options ONNX Runtime reports applying to every session Nervix builds.
//!
//! Layer: harness.
//! - **Owns.** The applied options of an inference session under each working-memory policy and
//!   of a binding inspection's session, read from ONNX Runtime's own report of them.
//! - **Depends on.** The production session options owner, the process's ONNX environment and the
//!   checked-in identity model.
//! - **Must not know.** Tensor plans, executor admission or model runs.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use nervix_models::InferencerWorkingMemory;
use nervix_primitives::sync::{StdArc, blocking::Mutex};
use ort::{logging::LogLevel, session::builder::SessionBuilder};

use super::OnnxSessionOptions;
use crate::runtime::onnx_environment;

/// The options ONNX Runtime reported applying to one session: the text of the report it logs at
/// Info level when it creates the session, `Session Options { … }`.
struct AppliedOptions {
    report: String,
}

impl AppliedOptions {
    /// Creates a session of the checked-in identity model from `builder`, with a session logger at
    /// Info, the level ONNX Runtime reports a session's options at, and keeps that one report.
    fn of(builder: SessionBuilder) -> Self {
        let reports = StdArc::new(Mutex::new(Vec::<String>::new()));
        let sink = reports.clone();
        let logger: ort::logging::LoggerFunction =
            StdArc::new(move |_level, _category, _id, _site, message: &str| {
                if message.starts_with("Session Options {") {
                    sink.lock().push(message.to_owned());
                }
            });
        let mut builder = builder
            .with_log_level(LogLevel::Info)
            .map_err(ort::Error::<()>::from)
            .expect("a session accepts the Info log level")
            .with_logger(logger)
            .map_err(ort::Error::<()>::from)
            .expect("a session accepts a logger of its own");
        let model = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/onnx/matrix_identity.onnx");
        let session = builder
            .commit_from_file(&model)
            .expect("the checked-in identity model loads");
        drop(session);
        let mut reports = std::mem::take(&mut *reports.lock());
        assert_eq!(
            reports.len(),
            1,
            "ONNX Runtime reports a session's options exactly once: {reports:?}"
        );
        Self {
            report: reports.pop().expect("exactly one report was checked above"),
        }
    }

    /// The number the report writes after `name:` the first time it names it, such as
    /// `enable_cpu_mem_arena:0`.
    fn number(report: &str, name: &str) -> i64 {
        let marker = format!("{name}:");
        let start = report
            .find(&marker)
            .unwrap_or_else(|| panic!("the report names {name}: {report}"));
        let value = report[start + marker.len()..].trim_start();
        let digits = value
            .split(|character: char| !(character.is_ascii_digit() || character == '-'))
            .next()
            .unwrap_or_default();
        digits
            .parse()
            .unwrap_or_else(|_| panic!("{name} carries a number in: {report}"))
    }

    fn option(&self, name: &str) -> i64 {
        Self::number(&self.report, name)
    }

    /// The thread count of the pool `pool` (`intra_op_param` or `inter_op_param`).
    fn pool_threads(&self, pool: &str) -> i64 {
        let marker = format!("{pool}:OrtThreadPoolParams {{");
        let start = self
            .report
            .find(&marker)
            .unwrap_or_else(|| panic!("the report names {pool}: {}", self.report));
        Self::number(&self.report[start + marker.len()..], "thread_pool_size")
    }

    /// The options every session shares, whatever its working memory: one thread in each pool
    /// and ONNX Runtime's full graph optimization, which it reports as level 4.
    fn assert_shared(&self) {
        assert_eq!(self.pool_threads("intra_op_param"), 1, "{}", self.report);
        assert_eq!(self.pool_threads("inter_op_param"), 1, "{}", self.report);
        assert_eq!(
            self.option("graph_optimization_level"),
            4,
            "{}",
            self.report
        );
    }

    /// Whether the session keeps the CPU memory arena and memory patterns, which move together.
    fn keeps_working_memory(&self) -> bool {
        let arena = self.option("enable_cpu_mem_arena");
        let pattern = self.option("enable_mem_pattern");
        assert_eq!(
            arena, pattern,
            "the arena and memory patterns follow one policy together: {}",
            self.report
        );
        arena == 1
    }
}

fn environment() -> &'static ort::environment::Environment {
    onnx_environment().expect("the process creates its ONNX Runtime environment")
}

#[test]
fn a_released_session_keeps_no_memory_arena_and_no_memory_pattern() {
    let applied = AppliedOptions::of(
        OnnxSessionOptions::inference(environment(), InferencerWorkingMemory::Released)
            .expect("a released session builder is created"),
    );
    applied.assert_shared();
    assert!(!applied.keeps_working_memory(), "{}", applied.report);
}

#[test]
fn a_retained_session_keeps_its_memory_arena_and_memory_patterns() {
    let applied = AppliedOptions::of(
        OnnxSessionOptions::inference(environment(), InferencerWorkingMemory::Retained)
            .expect("a retained session builder is created"),
    );
    applied.assert_shared();
    assert!(applied.keeps_working_memory(), "{}", applied.report);
}

#[test]
fn a_binding_inspection_session_keeps_no_working_memory() {
    let applied = AppliedOptions::of(
        OnnxSessionOptions::inspection(environment())
            .expect("an inspection session builder is created"),
    );
    applied.assert_shared();
    assert!(!applied.keeps_working_memory(), "{}", applied.report);
}
