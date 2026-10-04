from __future__ import annotations

import io
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.check_primitive_boundary import main

WORKSPACE = """
[workspace]
members = ["crates/primitives", "crates/harness", "crates/engine", "crates/vocabulary"]

[workspace.dependencies]
loom = "0.7.2"
nervix-model-harness = { path = "crates/harness" }
nervix-primitives = { path = "crates/primitives" }
nervix-vocabulary = { path = "crates/vocabulary" }
shuttle = "0.9"
"""

PRIMITIVES = """
[package]
name = "nervix-primitives"

[features]
loom = ["dep:loom"]
shuttle = ["dep:shuttle"]
turmoil = []

[dependencies]
loom = { workspace = true, optional = true }
shuttle = { workspace = true, optional = true }
"""

HARNESS = """
[package]
name = "nervix-model-harness"

[features]
loom = ["dep:loom", "nervix-primitives/loom"]

[dependencies]
loom = { workspace = true, optional = true }
nervix-primitives = { workspace = true }
"""

ENGINE = """
[package]
name = "nervix-engine"

[features]
shuttle = ["dep:shuttle", "nervix-primitives/shuttle", "nervix-vocabulary/shuttle"]

[dependencies]
nervix-primitives = { workspace = true }
nervix-vocabulary = { workspace = true }
shuttle = { workspace = true, optional = true }
"""

VOCABULARY = """
[package]
name = "nervix-vocabulary"

[features]
shuttle = ["nervix-primitives/shuttle"]

[dependencies]
nervix-primitives = { workspace = true }
"""

APPROVED = """
use nervix_primitives::sync::atomic::{AtomicBool, Ordering};
use nervix_primitives::sync::atomic as atomics;

fn publish(flag: &AtomicBool) {
    flag.store(true, Ordering::Release);
    nervix_primitives::sync::atomic::fence(Ordering::SeqCst);
    let _count = atomics::AtomicU64::new(0);
}
"""

PERMISSION = """
[[permission]]
path = "crates/engine/src/runner.rs"
items = ["sync::atomic::AtomicUsize", "sync::atomic::Ordering"]
owner = "The engine's model runner."
reason = "The statistic spans model executions."
limit = "Runner bookkeeping only."
"""

RUNNER = """
use nervix_primitives::unmodeled::sync::atomic::{AtomicUsize, Ordering};

fn count(executions: &AtomicUsize) {
    executions.fetch_add(1, Ordering::Relaxed);
}
"""

BLOCKING_PERMISSION = """
[[permission]]
path = "crates/engine/src/client.rs"
items = ["task::spawn_blocking"]
owner = "The engine's client tool."
reason = "The client is not a node and has no executor."
bound = "One read of a file the operator named."
"""

CLIENT = """
fn read(path: Path) { let _read = nervix_primitives::task::spawn_blocking(move || read(path)); }
"""

PROMPT_PERMISSION = """
[[permission]]
path = "crates/engine/src/prompt.rs"
items = ["task::block_in_place"]
owner = "The engine's completion prompt."
reason = "The prompt is a client tool that asks for one completion at a time."
bound = "One completion request, which ends by the client's retry deadline."
"""

PROMPT = """
fn complete(handle: Handle) -> Vec<String> {
    nervix_primitives::task::block_in_place(|| handle.block_on(suggest()))
}
"""


def write_repository(
    root: Path,
    sources: dict[str, str],
    manifests: dict[str, str] | None = None,
    permissions: str = PERMISSION,
    blocking_permissions: str = BLOCKING_PERMISSION,
) -> None:
    files = {
        "Cargo.toml": WORKSPACE,
        "crates/primitives/Cargo.toml": PRIMITIVES,
        "crates/primitives/src/sync.rs": "pub use std::sync::atomic::AtomicBool;\n",
        "crates/primitives/unmodeled-permissions.toml": permissions,
        "crates/primitives/blocking-permissions.toml": blocking_permissions,
        "crates/harness/Cargo.toml": HARNESS,
        "crates/engine/Cargo.toml": ENGINE,
        "crates/engine/src/runner.rs": RUNNER,
        "crates/engine/src/client.rs": CLIENT,
        "crates/vocabulary/Cargo.toml": VOCABULARY,
        **(manifests or {}),
        **sources,
    }
    for path, body in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)


def run(root: Path) -> tuple[int, str]:
    out = io.StringIO()
    with redirect_stdout(out), redirect_stderr(out):
        status = main(["--root", str(root)])
    return status, out.getvalue()


class CheckTestCase(unittest.TestCase):
    def check(
        self,
        sources: dict[str, str],
        manifests: dict[str, str] | None = None,
        permissions: str = PERMISSION,
        blocking_permissions: str = BLOCKING_PERMISSION,
    ) -> tuple[int, str]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        write_repository(root, sources, manifests, permissions, blocking_permissions)
        return run(root)

    def assert_rejected(self, source: str, *fragments: str) -> str:
        status, report = self.check({"crates/engine/src/lib.rs": source})
        self.assertEqual(status, 1, report)
        for fragment in fragments:
            self.assertIn(fragment, report)
        return report


class BoundaryTests(CheckTestCase):
    def test_approved_paths_pass(self) -> None:
        status, report = self.check({"crates/engine/src/lib.rs": APPROVED})
        self.assertEqual(status, 0, report)

    def test_the_owner_itself_may_name_the_backends(self) -> None:
        status, report = self.check(
            {"crates/primitives/src/lib.rs": "pub use std::sync::atomic::{AtomicU8, fence};\n"}
        )
        self.assertEqual(status, 0, report)

    def test_a_direct_import_fails_and_names_the_replacement(self) -> None:
        self.assert_rejected(
            "use std::sync::atomic::{AtomicU64, Ordering};\n",
            "crates/engine/src/lib.rs:1",
            "`std::sync::atomic::AtomicU64` bypasses the boundary",
            "use `nervix_primitives::sync::atomic::AtomicU64`",
        )

    def test_a_renamed_import_fails(self) -> None:
        self.assert_rejected(
            "use std::sync::atomic::AtomicBool as Flag;\n",
            "`std::sync::atomic::AtomicBool` bypasses the boundary",
        )

    def test_the_diagnostic_detector_is_named_only_through_the_boundary(self) -> None:
        self.assert_rejected(
            "use deloxide::{Mutex, RwLockReadGuard};\n"
            "fn start() { deloxide::Deloxide::new().start(); }\n",
            "crates/engine/src/lib.rs:1",
            "`deloxide::Mutex` bypasses the boundary",
            "use `nervix_primitives::sync::blocking::Mutex`",
            "`deloxide::RwLockReadGuard` bypasses the boundary",
            "use `nervix_primitives::sync::blocking::RwLockReadGuard`",
            "`deloxide::Deloxide::new` bypasses the boundary",
            "use `nervix_primitives::deadlock`",
        )

    def test_an_imported_atomic_module_fails(self) -> None:
        self.assert_rejected(
            "use core::sync::atomic as atomics;\n",
            "`core::sync::atomic` bypasses the boundary",
        )

    def test_a_grouped_import_fails(self) -> None:
        self.assert_rejected(
            "use std::{collections::VecDeque, sync::{Arc, atomic::{AtomicUsize, Ordering}}};\n",
            "`std::sync::atomic::AtomicUsize` bypasses the boundary",
            "`std::sync::atomic::Ordering` bypasses the boundary",
        )

    def test_a_fully_qualified_path_fails(self) -> None:
        self.assert_rejected(
            "fn fence() { ::std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst); }\n",
            "`std::sync::atomic` bypasses the boundary",
        )

    def test_a_glob_over_the_sync_module_fails(self) -> None:
        self.assert_rejected(
            "use std::sync::*;\nfn f() -> atomic::AtomicBool { atomic::AtomicBool::new(false) }\n",
            "`use std::sync::*` brings the atomic module into scope",
        )

    def test_a_renamed_crate_fails(self) -> None:
        report = self.assert_rejected(
            "extern crate std as standard;\n"
            "fn f() { standard::sync::atomic::fence(standard::sync::atomic::Ordering::SeqCst); }\n",
            "`extern crate std as standard` hides its atomic module",
            "`standard::sync::atomic` bypasses the boundary",
        )
        self.assertIn("crates/engine/src/lib.rs:2", report)

    def test_a_renamed_sync_module_fails(self) -> None:
        self.assert_rejected(
            "use std::sync as synchronization;\n"
            "fn f() -> synchronization::atomic::AtomicU8 { todo() }\n",
            "`synchronization::atomic` reaches the atomic module through a renamed `sync`",
        )

    def test_an_imported_sync_module_fails_where_it_reaches_atomics(self) -> None:
        self.assert_rejected(
            "use std::sync;\nfn f() -> sync::atomic::AtomicU8 { todo() }\n",
            "`sync::atomic` reaches the atomic module through an imported `sync`",
        )

    def test_a_macro_body_fails(self) -> None:
        self.assert_rejected(
            "macro_rules! flag {\n"
            "    ($krate:ident) => { $krate::sync::atomic::AtomicBool::new(false) };\n"
            "}\n",
            "`$krate::sync::atomic` bypasses the boundary",
        )

    def test_modeled_backends_fail_outside_the_owner(self) -> None:
        self.assert_rejected(
            '#[cfg(feature = "shuttle")]\nuse shuttle::sync::atomic::AtomicBool;\n'
            '#[cfg(feature = "loom")]\nfn f() { loom::sync::atomic::fence(Ordering::SeqCst); }\n',
            "`shuttle::sync::atomic::AtomicBool` bypasses the boundary",
            "`loom::sync::atomic` bypasses the boundary",
        )

    def test_comments_and_strings_are_not_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "// use std::sync::atomic::AtomicBool;\n"
                    "/// See `std::sync::atomic::Ordering`.\n"
                    'const TEXT: &str = "std::sync::atomic::fence";\n'
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_tests_benchmarks_and_new_files_are_checked(self) -> None:
        status, report = self.check(
            {
                "crates/engine/tests/harness.rs": "use std::sync::atomic::AtomicBool;\n",
                "crates/engine/benches/probe.rs": "use std::sync::atomic::AtomicUsize;\n",
            }
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/engine/tests/harness.rs:1", report)
        self.assertIn("crates/engine/benches/probe.rs:1", report)

    def test_a_renamed_boundary_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives as primitives;\n",
            "`nervix_primitives` is renamed to `primitives`",
        )


class StaticTests(CheckTestCase):
    def test_a_static_selected_atomic_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives::sync::atomic::{AtomicU64, Ordering};\n"
            "static NEXT: AtomicU64 = AtomicU64::new(1);\n",
            "crates/engine/src/lib.rs:2",
            "static `NEXT` holds a selected atomic, which outlives every model execution",
            "nervix_primitives::unmodeled::sync::atomic",
        )

    def test_a_static_behind_a_wrapper_or_an_array_fails(self) -> None:
        self.assert_rejected(
            "use std::sync::LazyLock;\n"
            "use nervix_primitives::sync::atomic::AtomicUsize;\n"
            "static COUNTS: LazyLock<[AtomicUsize; 4]> = LazyLock::new(Default::default);\n",
            "static `COUNTS` holds a selected atomic",
        )

    def test_a_static_named_through_a_module_path_fails(self) -> None:
        report = self.assert_rejected(
            "use nervix_primitives::sync::atomic as atomics;\n"
            "static FLAG: atomics::AtomicBool = atomics::AtomicBool::new(false);\n"
            "static SEEN: &nervix_primitives::sync::atomic::AtomicU8 = &LATEST;\n",
            "static `FLAG` holds a selected atomic",
            "static `SEEN` holds a selected atomic",
        )
        self.assertIn("crates/engine/src/lib.rs:3", report)

    def test_a_renamed_or_aliased_selected_atomic_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives::sync::atomic::{AtomicU32, AtomicU64 as Sequence};\n"
            "type Counter = AtomicU32;\n"
            "type Counters = [Counter; 2];\n"
            "static NEXT: Sequence = Sequence::new(0);\n"
            "static COUNTS: Counters = [Counter::new(0), Counter::new(0)];\n",
            "static `NEXT` holds a selected atomic",
            "static `COUNTS` holds a selected atomic",
        )

    def test_a_bare_name_the_file_does_not_import_counts_as_selected(self) -> None:
        self.assert_rejected(
            "use super::*;\nstatic mut NEXT: AtomicI16 = AtomicI16::new(0);\n",
            "static `NEXT` holds a selected atomic",
        )

    def test_a_thread_local_selected_atomic_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives::sync::atomic::AtomicIsize;\n"
            "nervix_primitives::thread_local! {\n"
            "    static SEEN: AtomicIsize = const { AtomicIsize::new(0) };\n"
            "}\n",
            "crates/engine/src/lib.rs:3",
            "static `SEEN` holds a selected atomic",
        )

    def test_a_static_initializer_that_constructs_a_selected_atomic_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives::sync::atomic::AtomicBool;\n"
            "struct Gate { open: AtomicBool }\n"
            "static GATE: Gate = Gate { open: AtomicBool::new(false) };\n",
            "static `GATE` constructs a selected atomic",
        )

    def test_a_const_fn_that_constructs_a_selected_atomic_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives::sync::atomic::AtomicI64;\n"
            "struct Watermark { unix_nanos: AtomicI64 }\n"
            "impl Watermark {\n"
            "    const fn new() -> Self {\n"
            "        Self { unix_nanos: AtomicI64::new(i64::MIN) }\n"
            "    }\n"
            "}\n",
            "crates/engine/src/lib.rs:4",
            "const fn `new` constructs a selected atomic",
            "make the function non-const",
        )

    def test_a_const_selected_atomic_fails(self) -> None:
        self.assert_rejected(
            "use nervix_primitives::sync::atomic::AtomicPtr;\n"
            "const EMPTY: AtomicPtr<u8> = AtomicPtr::<u8>::new(std::ptr::null_mut());\n"
            "fn f() { let _ = const { nervix_primitives::sync::atomic::AtomicU8::new(0) }; }\n",
            "const `EMPTY` makes a selected atomic in a const context",
            "a const block constructs a selected atomic",
        )

    def test_a_static_real_atomic_passes(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/runner.rs": RUNNER
                + "static RUNS: AtomicUsize = AtomicUsize::new(0);\n"
                + "static LAST: nervix_primitives::unmodeled::sync::atomic::AtomicUsize =\n"
                + "    nervix_primitives::unmodeled::sync::atomic::AtomicUsize::new(0);\n"
            }
        )
        self.assertEqual(status, 0, report)

    def test_statics_and_const_items_without_a_selected_atomic_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "use std::cell::Cell;\n"
                    "use nervix_primitives::sync::atomic::{AtomicU64, Ordering};\n"
                    "static NAME: &'static str = \"AtomicU64::new\";\n"
                    "nervix_primitives::thread_local! {\n"
                    "    static SEEN: Cell<usize> = const { Cell::new(0) };\n"
                    "}\n"
                    "struct Ring<const N: usize> { slots: [u64; N] }\n"
                    "struct Counter { value: AtomicU64 }\n"
                    "impl Counter {\n"
                    "    fn new() -> Self { Self { value: AtomicU64::new(0) } }\n"
                    "    const fn width() -> usize { 8 }\n"
                    "    fn read(counter: &'static AtomicU64) -> u64 { counter.load(Ordering::Relaxed) }\n"
                    "}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)


class PermissionTests(CheckTestCase):
    def test_a_permitted_unmodeled_use_passes(self) -> None:
        status, report = self.check({})
        self.assertEqual(status, 0, report)

    def test_an_unmodeled_use_without_a_permission_fails(self) -> None:
        status, report = self.check(
            {"crates/engine/src/lib.rs": "use nervix_primitives::unmodeled::sync::atomic::AtomicBool;\n"}
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/engine/src/lib.rs:1", report)
        self.assertIn("unmodeled items need a permission", report)

    def test_an_item_the_permission_does_not_list_fails(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/runner.rs": RUNNER
                + "fn flag() { let _ = nervix_primitives::unmodeled::sync::atomic::AtomicBool::new(false); }\n"
            }
        )
        self.assertEqual(status, 1)
        self.assertIn("does not list unmodeled `sync::atomic::AtomicBool`", report)

    def test_an_unused_permission_is_stale(self) -> None:
        status, report = self.check(
            {"crates/engine/src/runner.rs": "fn count() {}\n"},
        )
        self.assertEqual(status, 1)
        self.assertIn("stale permission: crates/engine/src/runner.rs uses no unmodeled item", report)

    def test_an_unused_permitted_item_is_stale(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/runner.rs": (
                    "use nervix_primitives::unmodeled::sync::atomic::AtomicUsize;\n"
                )
            }
        )
        self.assertEqual(status, 1)
        self.assertIn("does not use unmodeled `sync::atomic::Ordering`", report)

    def test_a_permission_without_a_reason_fails(self) -> None:
        status, report = self.check({}, permissions=PERMISSION.replace('reason = "The statistic spans model executions."\n', ""))
        self.assertEqual(status, 1)
        self.assertIn("needs `reason`", report)

    def test_the_unmodeled_module_must_be_imported_by_item(self) -> None:
        status, report = self.check(
            {"crates/engine/src/lib.rs": "use nervix_primitives::unmodeled::sync::atomic;\n"}
        )
        self.assertEqual(status, 1)
        self.assertIn("import unmodeled items by name", report)


class ManifestTests(CheckTestCase):
    def test_a_loom_dependency_outside_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + "\n[dev-dependencies]\nloom = { workspace = true }\n"
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/vocabulary/Cargo.toml", report)
        self.assertIn("depend on them instead of `loom`", report)

    def test_a_mandatory_loom_dependency_of_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/primitives/Cargo.toml": PRIMITIVES.replace(
                    "loom = { workspace = true, optional = true }", "loom = { workspace = true }"
                ).replace('loom = ["dep:loom"]', "loom = []")
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("`loom` must be an optional dependencies entry", report)

    def test_a_mode_not_forwarded_to_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY.replace(
                    'shuttle = ["nervix-primitives/shuttle"]', "shuttle = []"
                )
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("feature `shuttle` does not forward `nervix-primitives/shuttle`", report)

    def test_a_mode_without_a_direct_owner_dependency_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": (
                    '[package]\nname = "nervix-vocabulary"\n\n[features]\nturmoil = []\n'
                )
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("depends on nervix-primitives directly", report)

    def test_a_mode_not_forwarded_to_a_dependency_that_owns_it_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/engine/Cargo.toml": ENGINE.replace(', "nervix-vocabulary/shuttle"', "")
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("does not forward `nervix-vocabulary/shuttle`", report)

    def test_a_deloxide_dependency_outside_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + '\n[dependencies.deloxide]\nversion = "=1.1.0"\noptional = true\n'
            },
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/vocabulary/Cargo.toml: primitive boundary: only nervix-primitives depends on "
            "`deloxide`",
            report,
        )

    def test_a_mandatory_deloxide_dependency_of_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/primitives/Cargo.toml": PRIMITIVES
                + '[target.\'cfg(not(target_family = "wasm"))\'.dependencies]\n'
                'deloxide = "=1.1.0"\n'
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("`deloxide` must be an optional dependencies entry", report)

    def test_an_optional_deloxide_dependency_of_the_owner_passes(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/primitives/Cargo.toml": PRIMITIVES.replace(
                    "turmoil = []", 'turmoil = []\ndeloxide = ["dep:deloxide"]'
                )
                + "deloxide = { version = \"=1.1.0\", optional = true }\n"
            },
        )
        self.assertEqual(status, 0, report)

    def test_the_diagnostic_mode_not_forwarded_to_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY.replace(
                    'shuttle = ["nervix-primitives/shuttle"]',
                    'shuttle = ["nervix-primitives/shuttle"]\ndeloxide = []',
                )
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("feature `deloxide` does not forward `nervix-primitives/deloxide`", report)


class FamilyTests(CheckTestCase):
    """The families beyond atomics: async and thread-blocking synchronization, tasks, the runtime
    and its macros, streams, publication, concurrent collections, threads and model backends."""

    def test_approved_family_paths_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "use nervix_primitives::sync::{Arc, StdArc, StdWeak};\n"
                    "use nervix_primitives::{sync::{Notify, blocking::Mutex, watch}, task};\n"
                    "nervix_primitives::thread_local! { static SEEN: u8 = const { 0 }; }\n"
                    "#[nervix_primitives::test]\n"
                    "async fn waits() {\n"
                    "    task::consume_budget().await;\n"
                    "    nervix_primitives::select! {\n"
                    "        () = nervix_primitives::time::sleep(DURATION) => {}\n"
                    "    }\n"
                    "    tokio::pin!(future);\n"
                    "    let _pair = shuttle::future::block_on(async {});\n"
                    "}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_async_synchronization_fails_outside_the_boundary(self) -> None:
        self.assert_rejected(
            "use tokio::sync::{Notify, mpsc};\nuse tokio_util::sync::CancellationToken;\n",
            "`tokio::sync::Notify` bypasses the boundary; use `nervix_primitives::sync::Notify`",
            "`tokio::sync::mpsc` bypasses the boundary; use `nervix_primitives::sync::mpsc`",
            "`tokio_util::sync::CancellationToken` bypasses the boundary; use "
            "`nervix_primitives::sync::CancellationToken`",
        )

    def test_a_grouped_tokio_import_fails_only_for_the_governed_items(self) -> None:
        report = self.assert_rejected(
            "use tokio::{io::AsyncReadExt, sync::watch};\n",
            "`tokio::sync::watch` bypasses the boundary",
        )
        self.assertNotIn("tokio::io", report)

    def test_tasks_and_their_macros_fail_outside_the_boundary(self) -> None:
        self.assert_rejected(
            "#[tokio::test(flavor = \"multi_thread\")]\n"
            "async fn f() {\n"
            "    let handle = tokio::spawn(async {});\n"
            "    tokio::task::consume_budget().await;\n"
            "    tokio::select! { _ = handle => {} }\n"
            "}\n"
            "tokio::task_local! { static NOW: u8; }\n",
            "`tokio::test` bypasses the boundary; use `nervix_primitives::test`",
            "`tokio::spawn` bypasses the boundary; use `nervix_primitives::task::spawn`",
            "`tokio::task::consume_budget` bypasses the boundary; use "
            "`nervix_primitives::task::consume_budget`",
            "`tokio::select` bypasses the boundary; use `nervix_primitives::select`",
            "`tokio::task_local` bypasses the boundary; use "
            "`nervix_primitives::unmodeled::task_local`",
        )

    def test_blocking_synchronization_fails_outside_the_boundary(self) -> None:
        self.assert_rejected(
            "use parking_lot::Mutex;\n"
            "use std::sync::{Arc, OnceLock, mpsc};\n"
            "fn f() { let _lock = std::sync::Mutex::new(0); }\n",
            "`parking_lot::Mutex` bypasses the boundary; use "
            "`nervix_primitives::sync::blocking::Mutex`",
            "`std::sync::OnceLock` bypasses the boundary; use "
            "`nervix_primitives::sync::blocking::OnceLock`",
            "`std::sync::mpsc` bypasses the boundary; use `nervix_primitives::sync::blocking::mpsc`",
            "`std::sync::Mutex::new` bypasses the boundary",
        )

    def test_an_imported_sync_module_fails_where_it_reaches_a_governed_item(self) -> None:
        self.assert_rejected(
            "use std::sync;\nfn f() -> sync::Mutex<u8> { todo() }\nfn g() -> sync::Arc<u8> { todo() }\n",
            "`sync::Mutex` reaches `std::sync::Mutex` through an imported `std::sync` module; use "
            "`nervix_primitives::sync::blocking::Mutex`",
            "`sync::Arc` reaches `std::sync::Arc` through an imported `std::sync` module; use "
            "`nervix_primitives::sync::StdArc`",
        )

    def test_threads_and_thread_locals_fail_outside_the_boundary(self) -> None:
        self.assert_rejected(
            "use std::thread;\n"
            "fn f() { std::thread::spawn(|| {}); }\n"
            "thread_local! { static SEEN: u8 = const { 0 }; }\n",
            "`std::thread` bypasses the boundary; use `nervix_primitives::thread`",
            "`std::thread::spawn` bypasses the boundary; use `nervix_primitives::thread::spawn`",
            "`thread_local!` is the standard library's; use `nervix_primitives::thread_local!`",
        )

    def test_publication_collections_and_streams_fail_outside_the_boundary(self) -> None:
        self.assert_rejected(
            "use arc_swap::ArcSwap;\n"
            "use dashmap::{DashMap, mapref::entry::Entry};\n"
            "use concurrent_queue::ConcurrentQueue;\n"
            "use tokio_stream::wrappers::ReceiverStream;\n",
            "`arc_swap::ArcSwap` bypasses the boundary; use `nervix_primitives::publication::ArcSwap`",
            "`dashmap::DashMap` bypasses the boundary; use `nervix_primitives::collections::DashMap`",
            "`dashmap::mapref::entry::Entry` bypasses the boundary; use "
            "`nervix_primitives::collections::dash_map::Entry`",
            "`concurrent_queue::ConcurrentQueue` bypasses the boundary",
            "`tokio_stream::wrappers::ReceiverStream` bypasses the boundary; use "
            "`nervix_primitives::stream::wrappers::ReceiverStream`",
        )

    def test_modeled_primitives_fail_outside_the_boundary_while_runner_apis_pass(self) -> None:
        report = self.assert_rejected(
            "use shuttle::{sync::mpsc, thread};\n"
            "fn f() {\n"
            "    shuttle::future::spawn(async {});\n"
            "    shuttle::check_random(|| {}, 1);\n"
            "    let _switches = shuttle::current::context_switches();\n"
            "}\n",
            "`shuttle::sync::mpsc` bypasses the boundary; use `nervix_primitives::sync::blocking::mpsc`",
            "`shuttle::thread` bypasses the boundary; use `nervix_primitives::thread`",
            "`shuttle::future::spawn` bypasses the boundary; use `nervix_primitives::task::spawn`",
        )
        self.assertNotIn("check_random", report)
        self.assertNotIn("context_switches", report)

    def test_a_backend_alias_fails(self) -> None:
        self.assert_rejected(
            '#[cfg(feature = "shuttle")]\nextern crate shuttle_parking_lot as parking_lot;\n'
            "extern crate tokio as tokio_real;\n",
            "`extern crate shuttle_parking_lot as parking_lot` selects a backend outside the boundary",
            "`extern crate tokio as tokio_real` selects a backend outside the boundary",
        )

    def test_a_renamed_governed_crate_fails(self) -> None:
        self.assert_rejected(
            "use tokio as chitchat_tokio;\nfn f() -> chitchat_tokio::sync::Notify { todo() }\n",
            "`tokio` is renamed to `chitchat_tokio`, which hides the governed primitives below it",
        )

    def test_a_glob_over_a_governed_crate_fails(self) -> None:
        self.assert_rejected(
            "use tokio::*;\nuse parking_lot::*;\n",
            "`use tokio::*` brings governed primitives into scope",
            "`parking_lot::*` bypasses the boundary",
        )

    def test_a_governed_path_in_a_macro_body_fails(self) -> None:
        self.assert_rejected(
            "macro_rules! notify {\n    () => { tokio::sync::Notify::new() };\n}\n",
            "`tokio::sync::Notify::new` bypasses the boundary",
        )

    def test_an_inactive_cfg_branch_is_checked(self) -> None:
        self.assert_rejected(
            '#[cfg(any())]\nfn never() { let _lock = parking_lot::RwLock::new(0); }\n',
            "`parking_lot::RwLock::new` bypasses the boundary",
        )

    def test_every_spelling_of_the_diagnostic_detector_fails(self) -> None:
        self.assert_rejected(
            "use deloxide::Mutex as TrackedMutex;\n"
            "extern crate deloxide as detector;\n"
            "macro_rules! tracked {\n    () => { deloxide::RwLock::new(0) };\n}\n"
            '#[cfg(any())]\nfn never() { let _cycle = deloxide::Condvar::new(); }\n',
            "`deloxide::Mutex` bypasses the boundary",
            "use `nervix_primitives::sync::blocking::Mutex`",
            "`extern crate deloxide as detector` selects a backend outside the boundary",
            "`deloxide::RwLock::new` bypasses the boundary",
            "`deloxide::Condvar::new` bypasses the boundary",
        )

    def test_a_renamed_diagnostic_detector_dependency_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + '\n[dev-dependencies]\ndetector = { package = "deloxide", version = "=1.1.0" }\n'
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("only nervix-primitives depends on `deloxide`", report)
        self.assertIn(
            "the dev-dependencies entry `detector` renames `deloxide`, which hides its governed "
            "paths",
            report,
        )


class UnmodeledItemTests(CheckTestCase):
    def test_a_permitted_unmodeled_runtime_passes(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/binding.rs": (
                    "use nervix_primitives::unmodeled::runtime::Runtime;\n"
                    "fn start() -> Runtime {\n"
                    "    nervix_primitives::unmodeled::runtime::Builder::new_multi_thread()\n"
                    "        .build()\n"
                    "}\n"
                )
            },
            permissions=PERMISSION
            + """
[[permission]]
path = "crates/engine/src/binding.rs"
items = ["runtime::Builder", "runtime::Runtime"]
owner = "The engine's binding."
reason = "Its host enters the runtime."
limit = "The binding runs in no model."
""",
        )
        self.assertEqual(status, 0, report)

    def test_an_unmodeled_module_import_fails(self) -> None:
        status, report = self.check(
            {"crates/engine/src/lib.rs": "use nervix_primitives::unmodeled::runtime;\n"}
        )
        self.assertEqual(status, 1)
        self.assertIn("import unmodeled items by name", report)

    def test_an_unknown_unmodeled_path_fails(self) -> None:
        status, report = self.check(
            {"crates/engine/src/lib.rs": "fn f() { nervix_primitives::unmodeled::nothing(); }\n"}
        )
        self.assertEqual(status, 1)
        self.assertIn("name an unmodeled item by its path", report)


class OwnerOnlyManifestTests(CheckTestCase):
    def test_a_selected_library_outside_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + "parking_lot = { workspace = true }\n"
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("only nervix-primitives depends on `parking_lot`", report)

    def test_a_renamed_shuttle_wrapper_outside_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "Cargo.toml": WORKSPACE
                + 'shuttle-parking-lot = { package = "shuttle-parking_lot", version = "0.12" }\n',
                "crates/engine/Cargo.toml": ENGINE
                + "shuttle-parking-lot = { workspace = true, optional = true }\n",
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("only nervix-primitives depends on `shuttle-parking_lot`", report)


class LoomModelTests(CheckTestCase):
    """Loom models only atomics, its threads and thread-local storage; in a Loom build every other
    family is the ordinary library, so Loom model code may not name one."""

    def test_a_model_of_atomics_and_threads_passes(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(all(test, feature = "loom"))]\n'
                    "mod loom_models {\n"
                    "    use nervix_primitives::{sync::atomic::{AtomicUsize, Ordering}, thread};\n"
                    "    fn model() {\n"
                    "        let handle = thread::spawn(|| AtomicUsize::new(0));\n"
                    "        thread::yield_now();\n"
                    "        nervix_primitives::thread::park();\n"
                    "    }\n"
                    "}\n"
                )
            },
            blocking_permissions=BLOCKING_PERMISSION + (
                '\n[[permission]]\npath = "crates/engine/src/lib.rs"\n'
                'items = ["thread::spawn"]\nowner = "The engine model."\n'
                'reason = "A model needs scheduler-visible participants."\n'
                'bound = "One participant per finite model execution."\n'
            ),
        )
        self.assertEqual(status, 0, report)

    def test_an_unmodeled_family_in_a_model_fails(self) -> None:
        self.assert_rejected(
            '#[cfg(all(test, feature = "loom"))]\n'
            "mod loom_models {\n"
            "    use nervix_primitives::sync::Notify;\n"
            "    fn model() {\n"
            "        let map = nervix_primitives::collections::DashMap::<u8, u8>::new();\n"
            "    }\n"
            "}\n",
            "Loom model code names `nervix_primitives::sync::Notify`",
            "Loom model code names `nervix_primitives::collections::DashMap`",
        )

    def test_a_thread_operation_loom_does_not_model_fails_through_an_alias(self) -> None:
        self.assert_rejected(
            '#[cfg(feature = "loom")]\n'
            "mod loom_models {\n"
            "    use nervix_primitives::thread;\n"
            "    fn model() { thread::sleep(DELAY); }\n"
            "}\n",
            "Loom model code names `nervix_primitives::thread::sleep`",
        )

    def test_a_module_compiled_without_loom_is_not_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(all(test, not(feature = "loom")))]\n'
                    "mod tests {\n"
                    "    use nervix_primitives::sync::Notify;\n"
                    "}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)


class TimerAndNetworkTests(CheckTestCase):
    """Timers, the monotonic clock and sockets follow the build's execution mode, and names resolve
    through the node's resolver."""

    def test_approved_timer_and_socket_paths_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "use std::{net::{IpAddr, SocketAddr}, time::{self, Duration}};\n"
                    "use nervix_primitives::{\n"
                    "    net::{TcpListener, TcpStream},\n"
                    "    time::{Instant, sleep, timeout},\n"
                    "};\n"
                    "use tokio::io::AsyncReadExt as _;\n"
                    "fn f() -> time::Duration {\n"
                    "    let _started = Instant::now();\n"
                    "    let _simulation = turmoil::Builder::new().build();\n"
                    "    let _address = turmoil::lookup(\"server\");\n"
                    "    time::Duration::ZERO\n"
                    "}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_tokio_timers_fail_and_name_their_replacement(self) -> None:
        self.assert_rejected(
            "use tokio::time::{Instant, timeout};\n"
            "use tokio::time::Duration;\n"
            "fn f() { tokio::time::sleep(DURATION); }\n",
            "`tokio::time::Instant` bypasses the boundary; use `nervix_primitives::time::Instant`",
            "`tokio::time::timeout` bypasses the boundary; use `nervix_primitives::time::timeout`",
            "`tokio::time::Duration` bypasses the boundary; use `std::time::Duration`",
            "`tokio::time::sleep` bypasses the boundary; use `nervix_primitives::time::sleep`",
        )

    def test_the_standard_monotonic_clock_fails(self) -> None:
        self.assert_rejected(
            "use std::time::Instant;\nfn f() { let _now = std::time::Instant::now(); }\n",
            "`std::time::Instant` bypasses the boundary; use `nervix_primitives::time::Instant`",
            "`std::time::Instant::now` bypasses the boundary",
        )

    def test_an_imported_time_module_fails_where_it_reaches_the_monotonic_clock(self) -> None:
        report = self.assert_rejected(
            "use std::time;\n"
            "fn f() -> time::Duration { let _now = time::Instant::now(); time::Duration::ZERO }\n",
            "`time::Instant::now` reaches `std::time::Instant::now` through an imported "
            "`std::time` module; use `nervix_primitives::time::Instant::now`",
        )
        self.assertNotIn("Duration", report)

    def test_sockets_outside_the_boundary_fail(self) -> None:
        self.assert_rejected(
            "use tokio::net::{TcpListener, UdpSocket};\n"
            "use turmoil::net::TcpStream;\n"
            "fn f() {\n"
            "    let _socket = std::net::UdpSocket::bind(ADDRESS);\n"
            "    let _local = std::os::unix::net::UnixStream::connect(PATH);\n"
            "}\n",
            "`tokio::net::TcpListener` bypasses the boundary; use `nervix_primitives::net::TcpListener`",
            "`tokio::net::UdpSocket` bypasses the boundary; use `nervix_primitives::net::UdpSocket`",
            "`turmoil::net::TcpStream` bypasses the boundary; use `nervix_primitives::net::TcpStream`",
            "`std::net::UdpSocket::bind` bypasses the boundary",
            "`std::os::unix::net::UnixStream::connect` bypasses the boundary; use "
            "`nervix_primitives::net::UnixStream::connect`",
        )

    def test_an_imported_net_module_fails_where_it_reaches_a_socket(self) -> None:
        report = self.assert_rejected(
            "use std::net;\n"
            "fn f(address: net::SocketAddr) { let _listener = net::TcpListener::bind(address); }\n",
            "`net::TcpListener::bind` reaches `std::net::TcpListener::bind` through an imported "
            "`std::net` module",
        )
        self.assertNotIn("SocketAddr", report)

    def test_resolving_around_the_node_resolver_fails(self) -> None:
        self.assert_rejected(
            "use std::net::ToSocketAddrs;\n"
            "use tokio::net::ToSocketAddrs as _;\n"
            "async fn f() { let _answers = tokio::net::lookup_host(HOST).await; }\n",
            "`std::net::ToSocketAddrs` resolves names around the node's resolver; use "
            "`nervix_dns::DnsResolver`",
            "`tokio::net::ToSocketAddrs` resolves names around the node's resolver",
            "`tokio::net::lookup_host` resolves names around the node's resolver; use "
            "`nervix_dns::DnsResolver`",
        )

    def test_the_timer_alias_fails(self) -> None:
        self.assert_rejected(
            "extern crate turmoil as simulator;\n",
            "`extern crate turmoil as simulator` selects a backend outside the boundary",
        )

    def test_a_renamed_time_module_fails(self) -> None:
        self.assert_rejected(
            "use std::time as clock;\n",
            "`std::time` is renamed to `clock`, which hides the governed primitives below it",
        )

    def test_timers_and_sockets_in_loom_model_code_fail(self) -> None:
        self.assert_rejected(
            '#[cfg(all(test, feature = "loom"))]\n'
            "mod loom_models {\n"
            "    use nervix_primitives::time::sleep;\n"
            "    fn model() {\n"
            "        let _stream = nervix_primitives::net::TcpStream::connect(ADDRESS);\n"
            "    }\n"
            "}\n",
            "Loom model code names `nervix_primitives::time::sleep`",
            "Loom model code names `nervix_primitives::net::TcpStream::connect`",
        )


class CpuJobTests(CheckTestCase):
    """The boundary's CPU-job mechanism belongs to the bounded executor."""

    EXECUTOR = "crates/execution/src/workers.rs"

    def test_the_executor_may_run_an_admitted_cpu_job(self) -> None:
        status, report = self.check(
            {
                self.EXECUTOR: (
                    "fn start(work: Work) { let _job = nervix_primitives::task::spawn_cpu(work); }\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_the_mechanism_fails_anywhere_else(self) -> None:
        report = self.assert_rejected(
            "use nervix_primitives::task::spawn_cpu;\n"
            "fn f(work: Work) { nervix_primitives::task::spawn_cpu(work); }\n",
            "`nervix_primitives::task::spawn_cpu` is the bounded executor's mechanism for an "
            "admitted CPU job; only crates/execution/src/workers.rs may name it",
        )
        self.assertIn("crates/engine/src/lib.rs:1", report)
        self.assertIn("crates/engine/src/lib.rs:2", report)

    def test_the_mechanism_fails_through_an_imported_task_module_or_a_glob(self) -> None:
        report = self.assert_rejected(
            "use nervix_primitives::task;\n"
            "use nervix_primitives::task::*;\n"
            "fn f(work: Work) { task::spawn_cpu(work); task::yield_now(); }\n",
            "`nervix_primitives::task::spawn_cpu` is the bounded executor's mechanism",
        )
        self.assertIn("crates/engine/src/lib.rs:2", report)
        self.assertIn("crates/engine/src/lib.rs:3", report)


class BlockingPoolTests(CheckTestCase):
    """The runtime's blocking pool belongs to the bounded executor and to the owners a permission
    declares."""

    EXECUTOR = "crates/execution/src/workers.rs"
    POOL = "`nervix_primitives::task::spawn_blocking` is the runtime's blocking pool"

    def test_the_executor_and_a_declared_owner_may_name_the_pool(self) -> None:
        status, report = self.check(
            {
                self.EXECUTOR: (
                    "fn start(work: Work) { let _job = nervix_primitives::task::spawn_blocking(work); }\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_an_undeclared_caller_fails_however_it_names_the_pool(self) -> None:
        report = self.assert_rejected(
            "fn f(work: Work) { nervix_primitives::task::spawn_blocking(work); }\n",
            self.POOL,
            "admit this work through the bounded executor in nervix-execution, or declare its "
            "owner, reason and bound in crates/primitives/blocking-permissions.toml",
        )
        self.assertIn("crates/engine/src/lib.rs:1", report)
        for source in (
            "use nervix_primitives::task::spawn_blocking;\n",
            "use nervix_primitives::task::{spawn, spawn_blocking as offload};\n",
            "use nervix_primitives::task;\nfn f(work: Work) { task::spawn_blocking(work); }\n",
            "use nervix_primitives::task as tasks;\nfn f(work: Work) { tasks::spawn_blocking(work); }\n",
            "use nervix_primitives::task::*;\n",
        ):
            with self.subTest(source=source):
                self.assert_rejected(source, self.POOL)

    def test_the_report_names_the_first_line_that_names_the_pool(self) -> None:
        report = self.assert_rejected(
            "fn f() {}\n"
            "use nervix_primitives::task::spawn_blocking;\n"
            "fn g(work: Work) { nervix_primitives::task::spawn_blocking(work); }\n",
            "crates/engine/src/lib.rs:2",
        )
        self.assertNotIn("crates/engine/src/lib.rs:3", report)

    def test_a_permission_for_a_file_that_no_longer_names_the_pool_is_stale(self) -> None:
        status, report = self.check({"crates/engine/src/client.rs": "fn read() {}\n"})
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/primitives/blocking-permissions.toml: stale permission: "
            "crates/engine/src/client.rs does not name `nervix_primitives::task::spawn_blocking`",
            report,
        )

    def test_a_permission_without_a_bound_fails(self) -> None:
        status, report = self.check(
            {},
            blocking_permissions=BLOCKING_PERMISSION.replace(
                'bound = "One read of a file the operator named."\n', ""
            ),
        )
        self.assertEqual(status, 1)
        self.assertIn("needs a non-empty `bound`", report)

    def test_a_permission_with_an_unknown_key_fails(self) -> None:
        status, report = self.check(
            {}, blocking_permissions=BLOCKING_PERMISSION + 'limit = "Nothing."\n'
        )
        self.assertEqual(status, 1)
        self.assertIn("has unknown keys: limit", report)

    def test_a_repeated_permission_fails(self) -> None:
        status, report = self.check(
            {}, blocking_permissions=BLOCKING_PERMISSION + BLOCKING_PERMISSION
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "permission #2 repeats `task::spawn_blocking` for crates/engine/src/client.rs", report
        )


class BlockInPlaceTests(CheckTestCase):
    """Blocking a runtime worker thread in place belongs to no file but the owners a permission
    declares."""

    EXECUTOR = "crates/execution/src/workers.rs"
    PROMPT_PATH = "crates/engine/src/prompt.rs"
    IN_PLACE = (
        "`nervix_primitives::task::block_in_place` is the runtime's way to block the worker thread "
        "that calls it"
    )

    def test_a_declared_owner_may_block_in_place(self) -> None:
        status, report = self.check(
            {self.PROMPT_PATH: PROMPT}, blocking_permissions=BLOCKING_PERMISSION + PROMPT_PERMISSION
        )
        self.assertEqual(status, 0, report)

    def test_an_undeclared_caller_fails_however_it_names_block_in_place(self) -> None:
        report = self.assert_rejected(
            PROMPT,
            self.IN_PLACE,
            "admit this work through the bounded executor in nervix-execution, or declare its "
            "owner, reason and bound in crates/primitives/blocking-permissions.toml",
        )
        self.assertIn("crates/engine/src/lib.rs:3", report)
        for source in (
            "use nervix_primitives::task::block_in_place;\n",
            "use nervix_primitives::{runtime::Handle, task::{spawn, block_in_place}};\n",
            "use nervix_primitives::task::block_in_place as wait_here;\n",
            "use nervix_primitives::task;\nfn f() { task::block_in_place(suggest); }\n",
            "use nervix_primitives::task as tasks;\nfn f() { tasks::block_in_place(suggest); }\n",
            "use nervix_primitives::task::*;\n",
        ):
            with self.subTest(source=source):
                self.assert_rejected(source, self.IN_PLACE)

    def test_the_executor_is_no_owner_of_block_in_place(self) -> None:
        status, report = self.check({self.EXECUTOR: PROMPT})
        self.assertEqual(status, 1)
        self.assertIn(f"{self.EXECUTOR}:3: primitive boundary: {self.IN_PLACE}", report)

    def test_a_permission_for_a_file_that_no_longer_blocks_in_place_is_stale(self) -> None:
        status, report = self.check(
            {self.PROMPT_PATH: "fn complete() {}\n"},
            blocking_permissions=BLOCKING_PERMISSION + PROMPT_PERMISSION,
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/primitives/blocking-permissions.toml: stale permission: "
            "crates/engine/src/prompt.rs does not name `nervix_primitives::task::block_in_place`",
            report,
        )


class BlockingItemTests(CheckTestCase):
    """A blocking permission declares its file for exactly the blocking items it lists."""

    PERMISSION_FOR_BOTH = BLOCKING_PERMISSION.replace(
        'items = ["task::spawn_blocking"]', 'items = ["task::spawn_blocking", "task::block_in_place"]'
    )
    PROMPT_IN_CLIENT = PROMPT_PERMISSION.replace(
        "crates/engine/src/prompt.rs", "crates/engine/src/client.rs"
    )

    def client_that_also_blocks_in_place(self) -> dict[str, str]:
        return {"crates/engine/src/client.rs": CLIENT + PROMPT}

    def test_a_permission_does_not_declare_an_item_it_does_not_list(self) -> None:
        status, report = self.check(self.client_that_also_blocks_in_place())
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/engine/src/client.rs:5: primitive boundary: "
            "`nervix_primitives::task::block_in_place` is the runtime's way to block the worker "
            "thread that calls it",
            report,
        )
        self.assertNotIn("is the runtime's blocking pool", report)
        self.assertNotIn("stale permission", report)

    def test_each_item_of_one_file_may_have_its_own_owner(self) -> None:
        status, report = self.check(
            self.client_that_also_blocks_in_place(),
            blocking_permissions=BLOCKING_PERMISSION + self.PROMPT_IN_CLIENT,
        )
        self.assertEqual(status, 0, report)

    def test_one_permission_may_list_several_items(self) -> None:
        status, report = self.check(
            self.client_that_also_blocks_in_place(), blocking_permissions=self.PERMISSION_FOR_BOTH
        )
        self.assertEqual(status, 0, report)

    def test_a_listed_item_the_file_does_not_name_is_stale(self) -> None:
        status, report = self.check({}, blocking_permissions=self.PERMISSION_FOR_BOTH)
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/primitives/blocking-permissions.toml: stale permission: "
            "crates/engine/src/client.rs does not name `nervix_primitives::task::block_in_place`",
            report,
        )
        self.assertNotIn("does not name `nervix_primitives::task::spawn_blocking`", report)

    def test_an_item_one_file_declares_twice_fails(self) -> None:
        for permissions in (
            BLOCKING_PERMISSION
            + self.PROMPT_IN_CLIENT.replace("task::block_in_place", "task::spawn_blocking"),
            BLOCKING_PERMISSION.replace(
                'items = ["task::spawn_blocking"]',
                'items = ["task::spawn_blocking", "task::spawn_blocking"]',
            ),
        ):
            with self.subTest(permissions=permissions):
                status, report = self.check({}, blocking_permissions=permissions)
                self.assertEqual(status, 1)
                self.assertIn(
                    "repeats `task::spawn_blocking` for crates/engine/src/client.rs", report
                )

    def test_a_permission_without_items_fails(self) -> None:
        for items in ("", "items = []\n", 'items = "task::spawn_blocking"\n'):
            with self.subTest(items=items):
                status, report = self.check(
                    {},
                    blocking_permissions=BLOCKING_PERMISSION.replace(
                        'items = ["task::spawn_blocking"]\n', items
                    ),
                )
                self.assertEqual(status, 1)
                self.assertIn(
                    "crates/primitives/blocking-permissions.toml permission #1 lists no items",
                    report,
                )

    def test_an_item_no_blocking_permission_declares_fails(self) -> None:
        for item in (
            '"task::spawn_cpu"',
            '"task::spawn"',
            '"nervix_primitives::task::spawn_blocking"',
            "1",
        ):
            with self.subTest(item=item):
                status, report = self.check(
                    {},
                    blocking_permissions=BLOCKING_PERMISSION.replace(
                        'items = ["task::spawn_blocking"]', f"items = [{item}]"
                    ),
                )
                self.assertEqual(status, 1)
                self.assertIn(
                    "; a blocking permission declares only",
                    report,
                )


class ThreadCreationTests(CheckTestCase):
    """Thread creation needs an exact owner even in tests, models and harnesses."""

    PATH = "crates/engine/src/threads.rs"
    ITEMS = (
        "thread::spawn",
        "thread::Builder",
        "thread::scope",
        "thread::spawn_detached",
        "unmodeled::thread::Builder",
    )

    def permission(self, path: str, items: tuple[str, ...]) -> str:
        names = ", ".join(f'"{item}"' for item in items)
        return (
            "\n[[permission]]\n"
            f'path = "{path}"\nitems = [{names}]\n'
            'owner = "The finite thread fixture."\n'
            'reason = "The fixture tests participants outside a node executor."\n'
            'bound = "Two participants joined before the fixture returns."\n'
        )

    def sources(self, item: str) -> tuple[str, ...]:
        module, name = item.rsplit("::", 1)
        prefix = f"nervix_primitives::{module}"
        forms = (
            f"fn f() {{ {prefix}::{name}(work); }}\n",
            f"use {prefix}::{name};\n",
            f"use {prefix}::{{{name} as participant}};\n",
            f"use {prefix}::{name} as participant;\n",
            f"use nervix_primitives::{{{module}::{{{name}}}}};\n",
        )
        if module == "thread":
            forms += (
                f"use {prefix};\nfn f() {{ thread::{name}(work); }}\n",
                f"use {prefix} as threads;\nfn f() {{ threads::{name}(work); }}\n",
            )
        return forms

    def unmodeled_permission(self, item: str, path: str) -> str:
        if item != "unmodeled::thread::Builder":
            return PERMISSION
        return PERMISSION + (
            "\n[[permission]]\n"
            f'path = "{path}"\nitems = ["thread::Builder"]\n'
            'owner = "The finite thread fixture."\n'
            'reason = "This participant runs outside every model."\n'
            'limit = "No protocol assertion relies on this thread."\n'
        )

    def test_each_item_requires_a_declaration_in_every_import_form(self) -> None:
        for item in self.ITEMS:
            for source in self.sources(item):
                with self.subTest(item=item, source=source):
                    status, report = self.check(
                        {self.PATH: source}, permissions=self.unmodeled_permission(item, self.PATH)
                    )
                    self.assertEqual(status, 1, report)
                    self.assertIn(f"{self.PATH}:", report)
                    self.assertIn("primitive boundary:", report)
                    self.assertIn(f"`nervix_primitives::{item}`", report)
                    self.assertIn("owner, reason and bound", report)

    def test_each_declared_item_passes_in_every_import_form(self) -> None:
        for item in self.ITEMS:
            for source in self.sources(item):
                with self.subTest(item=item, source=source):
                    status, report = self.check(
                        {self.PATH: source},
                        permissions=self.unmodeled_permission(item, self.PATH),
                        blocking_permissions=BLOCKING_PERMISSION + self.permission(self.PATH, (item,)),
                    )
                    self.assertEqual(status, 0, report)

    def test_a_glob_names_every_thread_creating_item(self) -> None:
        source = "use nervix_primitives::thread::*;\n"
        status, report = self.check({self.PATH: source})
        self.assertEqual(status, 1, report)
        for item in self.ITEMS[:-1]:
            self.assertIn(f"`nervix_primitives::{item}`", report)
        status, report = self.check(
            {self.PATH: source},
            blocking_permissions=BLOCKING_PERMISSION + self.permission(self.PATH, self.ITEMS[:-1]),
        )
        self.assertEqual(status, 0, report)

    def test_the_real_builder_needs_both_permissions(self) -> None:
        status, report = self.check(
            {self.PATH: "use nervix_primitives::unmodeled::thread::Builder;\n"},
            blocking_permissions=BLOCKING_PERMISSION
            + self.permission(self.PATH, ("unmodeled::thread::Builder",)),
        )
        self.assertEqual(status, 1, report)
        self.assertIn("unmodeled items need a permission", report)

    def test_parent_globs_and_reimported_module_aliases_keep_confinement(self) -> None:
        sources = (
            "use nervix_primitives::thread as threads;\nuse threads::spawn as participant;\n",
            "use threads::spawn;\nuse nervix_primitives::thread as threads;\n",
            "use actors::spawn;\nuse threads as actors;\nuse nervix_primitives::thread as threads;\n",
            "use nervix_primitives::thread as threads;\nuse self::threads::spawn;\n",
            "use nervix_primitives::thread as threads;\nfn f() { self::threads::spawn(work); }\n",
            "use nervix_primitives::thread as threads;\nuse super::threads as actors;\nuse actors::spawn;\n",
            "use nervix_primitives::thread as threads;\nuse crate::threads::spawn;\n",
            "use nervix_primitives::thread as threads;\nfn f() { crate::threads::spawn(work); }\n",
        )
        for source in sources:
            with self.subTest(source=source):
                status, report = self.check({self.PATH: source})
                self.assertEqual(status, 1, report)
                self.assertIn("primitive boundary: `nervix_primitives::thread::spawn`", report)
                status, report = self.check(
                    {self.PATH: source},
                    blocking_permissions=BLOCKING_PERMISSION + self.permission(self.PATH, ("thread::spawn",)),
                )
                self.assertEqual(status, 0, report)

    def test_a_root_glob_cannot_hide_selected_or_real_thread_creation(self) -> None:
        for path in ("thread::spawn", "unmodeled::thread::Builder::new"):
            with self.subTest(path=path):
                status, report = self.check(
                    {self.PATH: f"use nervix_primitives::*;\nfn f() {{ {path}(); }}\n"}
                )
                self.assertEqual(status, 1, report)
                self.assertIn("primitive boundary: import boundary modules and items by name", report)

    def test_local_module_globs_keep_their_own_namespace(self) -> None:
        for source in ("mod tests { use super::*; }\n", "mod inner { use super::super::*; }\n", "mod inner { use crate::*; }\n"):
            with self.subTest(source=source):
                status, report = self.check({self.PATH: source})
                self.assertEqual(status, 0, report)

    def test_a_reimported_thread_glob_names_all_its_confined_items(self) -> None:
        source = "use threads::*;\nuse nervix_primitives::thread as threads;\n"
        status, report = self.check({self.PATH: source})
        self.assertEqual(status, 1, report)
        for item in self.ITEMS[:-1]:
            self.assertIn(f"`nervix_primitives::{item}`", report)
        status, report = self.check(
            {self.PATH: source},
            blocking_permissions=BLOCKING_PERMISSION + self.permission(self.PATH, self.ITEMS[:-1]),
        )
        self.assertEqual(status, 0, report)

    def test_tests_models_and_harnesses_declare_each_file(self) -> None:
        contexts = (
            (self.PATH, "#[cfg(test)]\nmod tests { %s }\n"),
            (self.PATH, '#[cfg(all(test, feature = "loom"))]\nmod models { %s }\n'),
            (self.PATH, '#[cfg(all(test, feature = "shuttle"))]\nmod checks { %s }\n'),
            ("crates/harness/src/lib.rs", "%s\n"),
            ("tests/participants.rs", "%s\n"),
            ("benches/participants.rs", "%s\n"),
            ("examples/participants.rs", "%s\n"),
            (self.PATH, "#[cfg(any())]\nmod inactive { %s }\n"),
            (self.PATH, "macro_rules! participants { () => { %s } }\n"),
        )
        for path, context in contexts:
            with self.subTest(path=path, context=context):
                source = context % "use nervix_primitives::thread::spawn;"
                status, report = self.check({path: source})
                self.assertEqual(status, 1, report)
                self.assertIn("primitive boundary: `nervix_primitives::thread::spawn`", report)
                status, report = self.check(
                    {path: source},
                    blocking_permissions=BLOCKING_PERMISSION + self.permission(path, ("thread::spawn",)),
                )
                self.assertEqual(status, 0, report)

    def test_a_permission_declares_only_its_exact_file_and_items(self) -> None:
        status, report = self.check(
            {
                self.PATH: "use nervix_primitives::thread::{spawn, Builder};\n",
                "crates/execution/src/workers.rs": "use nervix_primitives::thread::spawn;\n",
            },
            blocking_permissions=BLOCKING_PERMISSION + self.permission(self.PATH, ("thread::spawn",)),
        )
        self.assertEqual(status, 1, report)
        self.assertIn(f"{self.PATH}:1: primitive boundary: `nervix_primitives::thread::Builder`", report)
        self.assertIn("crates/execution/src/workers.rs:1: primitive boundary:", report)

    def test_stale_thread_permissions_fail_for_every_item(self) -> None:
        for item in self.ITEMS:
            for sources in ({self.PATH: "fn f() {}\n"}, {}):
                with self.subTest(item=item, sources=sources):
                    status, report = self.check(
                        sources,
                        blocking_permissions=BLOCKING_PERMISSION + self.permission(self.PATH, (item,)),
                    )
                    self.assertEqual(status, 1, report)
                    self.assertIn(
                        f"stale permission: {self.PATH} does not name `nervix_primitives::{item}`",
                        report,
                    )

    def test_thread_permissions_require_owner_reason_and_bound(self) -> None:
        for field in ("owner", "reason", "bound"):
            with self.subTest(field=field):
                permission = self.permission(self.PATH, ("thread::spawn",))
                permission = "\n".join(line for line in permission.splitlines() if not line.startswith(field))
                status, report = self.check(
                    {self.PATH: "use nervix_primitives::thread::spawn;\n"},
                    blocking_permissions=BLOCKING_PERMISSION + permission,
                )
                self.assertEqual(status, 1, report)
                self.assertIn(f"needs a non-empty `{field}`", report)

    def test_thread_permissions_cover_one_rust_file(self) -> None:
        for path in ("crates/engine/src", "crates/engine/src/*.rs", "../participant.rs", "/participant.rs"):
            with self.subTest(path=path):
                status, report = self.check(
                    {}, blocking_permissions=BLOCKING_PERMISSION + self.permission(path, ("thread::spawn",))
                )
                self.assertEqual(status, 1, report)
                self.assertIn("a permission covers one Rust file", report)


class TurmoilManifestTests(CheckTestCase):
    """Only the owner selects Turmoil's network; a harness may run Turmoil behind its own feature."""

    HARNESS_USER = ENGINE.replace(
        'shuttle = ["dep:shuttle", "nervix-primitives/shuttle", "nervix-vocabulary/shuttle"]',
        'shuttle = ["dep:shuttle", "nervix-primitives/shuttle", "nervix-vocabulary/shuttle"]\n'
        'turmoil = ["dep:turmoil", "nervix-primitives/turmoil"]',
    ) + "turmoil = { workspace = true, optional = true }\n"

    def test_a_harness_that_runs_turmoil_behind_its_feature_passes(self) -> None:
        status, report = self.check(
            {}, manifests={"crates/engine/Cargo.toml": self.HARNESS_USER}
        )
        self.assertEqual(status, 0, report)

    def test_turmoil_without_the_packages_feature_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + "turmoil = { workspace = true, optional = true }\n"
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/vocabulary/Cargo.toml", report)
        self.assertIn("only nervix-primitives selects Turmoil's network", report)

    def test_a_mandatory_or_development_turmoil_dependency_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/engine/Cargo.toml": self.HARNESS_USER
                + "\n[dev-dependencies]\nturmoil = { workspace = true }\n"
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("`turmoil` must be an optional dependencies entry, not a dev-dependencies one", report)

    def test_the_shuttle_tokio_wrapper_outside_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/engine/Cargo.toml": ENGINE
                + "shuttle-tokio = { workspace = true, optional = true }\n"
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("only nervix-primitives depends on `shuttle-tokio`", report)


class TokioUnstableTests(CheckTestCase):
    """Tokio's unstable runtime controls belong to the Turmoil recipes."""

    JUSTFILE = (
        'cargo_target_dir := "target"\n'
        "\n"
        "# Runs the simulation with `--cfg tokio_unstable` scoped to it.\n"
        'test-turmoil budget_seconds="480":\n'
        '    RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" cargo test --features turmoil\n'
        "\n"
        "[parallel]\n"
        "lint: fmt\n"
        "    cargo clippy\n"
    )

    def test_the_turmoil_recipes_may_pass_the_cfg(self) -> None:
        status, report = self.check({}, manifests={"justfile": self.JUSTFILE})
        self.assertEqual(status, 0, report)

    def test_another_recipe_passing_the_cfg_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "justfile": self.JUSTFILE
                + 'test filter="":\n    RUSTFLAGS="--cfg=tokio_unstable" cargo test {{ filter }}\n'
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("justfile:11", report)
        self.assertIn("only a Turmoil recipe passes it, and `test` is not one", report)

    def test_cargo_configuration_workflows_and_build_scripts_fail(self) -> None:
        status, report = self.check(
            {"crates/engine/build.rs": 'fn main() { println!("cargo:rustc-cfg=tokio_unstable"); }\n'},
            manifests={
                ".cargo/config.toml": '[build]\nrustflags = ["--cfg", "tokio_unstable"]\n',
                ".github/workflows/check.yaml": (
                    "jobs:\n  tests:\n    env:\n      RUSTFLAGS: --cfg tokio_unstable\n"
                ),
            },
        )
        self.assertEqual(status, 1)
        self.assertIn(".cargo/config.toml:2", report)
        self.assertIn(".github/workflows/check.yaml:4", report)
        self.assertIn("crates/engine/build.rs", report)


class SharedOwnershipTests(CheckTestCase):
    """Shared ownership is the same library type in every mode, reached through the boundary."""

    def test_approved_shared_ownership_passes(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "use nervix_primitives::sync::{Arc, StdArc, StdWeak};\n"
                    "fn f() -> StdWeak<u8> {\n"
                    "    let owned = Arc::new(1_u8);\n"
                    "    let external = nervix_primitives::sync::StdArc::new(2_u8);\n"
                    "    StdArc::downgrade(&external)\n"
                    "}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_the_libraries_own_paths_fail_and_name_the_boundary(self) -> None:
        self.assert_rejected(
            "use triomphe::Arc;\n"
            "use std::sync::{Arc as StdArc, Weak};\n"
            "fn f() { let _shared = triomphe::Arc::new(0); let _std = std::sync::Arc::new(1); }\n",
            "`triomphe::Arc` bypasses the boundary; use `nervix_primitives::sync::Arc`",
            "`std::sync::Arc` bypasses the boundary; use `nervix_primitives::sync::StdArc`",
            "`std::sync::Weak` bypasses the boundary; use `nervix_primitives::sync::StdWeak`",
            "`triomphe::Arc::new` bypasses the boundary; use `nervix_primitives::sync::Arc::new`",
            "`std::sync::Arc::new` bypasses the boundary; use `nervix_primitives::sync::StdArc::new`",
        )

    def test_the_allocation_crates_and_modeled_references_fail(self) -> None:
        self.assert_rejected(
            "extern crate alloc;\n"
            "use alloc::sync::Arc;\n"
            "fn f() -> loom::sync::Arc<u8> { todo() }\n",
            "`alloc::sync::Arc` bypasses the boundary; use `nervix_primitives::sync::StdArc`",
            "`loom::sync::Arc` bypasses the boundary; use `nervix_primitives::sync::StdArc`",
        )

    def test_a_glob_or_a_renamed_crate_fails(self) -> None:
        self.assert_rejected(
            "use triomphe::*;\nuse triomphe as shared;\n",
            "`triomphe::*` bypasses the boundary",
            "`triomphe` bypasses the boundary; use `nervix_primitives::sync`",
        )

    def test_a_triomphe_dependency_outside_the_owner_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={"crates/vocabulary/Cargo.toml": VOCABULARY + 'triomphe = "0.1"\n'},
        )
        self.assertEqual(status, 1)
        self.assertIn("only nervix-primitives depends on `triomphe`", report)


class FuturesTests(CheckTestCase):
    """The `futures` crates' synchronization, executors, waker registration, `select!` and abort
    handles are governed; their pure combinators are not."""

    def test_the_futures_families_fail_and_name_the_boundary(self) -> None:
        self.assert_rejected(
            "use futures_channel::mpsc::unbounded;\n"
            "use futures::channel::oneshot;\n"
            "use futures_util::{lock::Mutex, task::AtomicWaker};\n"
            "use atomic_waker::AtomicWaker as Registration;\n"
            "use futures::future::{AbortHandle, Abortable, abortable};\n"
            "fn f() {\n"
            "    futures_executor::block_on(async {});\n"
            "    futures_util::select! { () = ready => {} }\n"
            "    futures::select_biased! { () = ready => {} }\n"
            "}\n",
            "`futures_channel::mpsc::unbounded` bypasses the boundary; use "
            "`nervix_primitives::sync::mpsc::unbounded`",
            "`futures::channel::oneshot` bypasses the boundary; use `nervix_primitives::sync::oneshot`",
            "`futures_util::lock::Mutex` bypasses the boundary; use `nervix_primitives::sync::Mutex`",
            "`futures_util::task::AtomicWaker` bypasses the boundary; use "
            "`nervix_primitives::sync::AtomicWaker`",
            "`atomic_waker::AtomicWaker` bypasses the boundary; use "
            "`nervix_primitives::sync::AtomicWaker`",
            "`futures::future::AbortHandle` bypasses the boundary; use "
            "`nervix_primitives::task::AbortHandle`",
            "`futures::future::Abortable` bypasses the boundary; use "
            "`nervix_primitives::sync::CancellationToken`",
            "`futures::future::abortable` bypasses the boundary",
            "`futures_executor::block_on` bypasses the boundary; use "
            "`nervix_primitives::runtime::block_on`",
            "`futures_util::select` bypasses the boundary; use `nervix_primitives::select`",
            "`futures::select_biased` bypasses the boundary; use `nervix_primitives::select`",
        )

    def test_pure_combinators_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "use futures_util::{FutureExt as _, StreamExt, stream::FuturesUnordered};\n"
                    "fn f() { let _all = futures_util::future::join_all(Vec::<Ready>::new()); }\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_a_program_outside_every_mode_reaches_the_futures_families_under_a_permission(
        self,
    ) -> None:
        status, report = self.check(
            {
                "crates/engine/src/console.rs": (
                    "use nervix_primitives::unmodeled::futures::{AbortHandle, mpsc};\n"
                    "async fn f() {\n"
                    "    let (sender, receiver) = mpsc::unbounded::<u8>();\n"
                    "    nervix_primitives::unmodeled::futures::select! { () = ready => {} }\n"
                    "}\n"
                )
            },
            permissions=PERMISSION
            + """
[[permission]]
path = "crates/engine/src/console.rs"
items = ["futures::AbortHandle", "futures::mpsc", "futures::select"]
owner = "The engine's browser console."
reason = "Its event loop runs in no execution mode."
limit = "Nothing it does is modeled."
""",
        )
        self.assertEqual(status, 0, report)


GUEST_SDK = """
[package]
name = "nervix-wasm-sdk"
"""

GUEST = """
[package]
name = "example-guest"

[lib]
crate-type = ["cdylib"]

[dependencies]
nervix-wasm-sdk = { path = "../../crates/sdk" }

[workspace]
"""


class GuestCodeTests(CheckTestCase):
    """Guest code is compiled into a user's WASM guest, where no execution mode exists."""

    def test_the_guest_sdk_and_its_guests_are_outside_the_source_rules(self) -> None:
        status, report = self.check(
            {
                "crates/sdk/src/lib.rs": "use std::sync::Arc;\n",
                "examples/guest/src/lib.rs": "use std::{ops::Range, sync::Arc};\n",
            },
            manifests={
                "crates/sdk/Cargo.toml": GUEST_SDK,
                "examples/guest/Cargo.toml": GUEST,
            },
        )
        self.assertEqual(status, 0, report)

    def test_a_host_crate_that_uses_the_sdk_is_governed(self) -> None:
        status, report = self.check(
            {
                "crates/sdk/src/lib.rs": "use std::sync::Arc;\n",
                "crates/engine/tests/guest.rs": "use std::sync::Arc;\n",
            },
            manifests={
                "crates/sdk/Cargo.toml": GUEST_SDK,
                "crates/engine/Cargo.toml": ENGINE + 'nervix-wasm-sdk = { path = "../sdk" }\n',
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/engine/tests/guest.rs:1", report)
        self.assertNotIn("crates/sdk/src/lib.rs", report)

    def test_a_guest_manifest_is_still_checked(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/sdk/Cargo.toml": GUEST_SDK + '\n[dependencies]\nparking_lot = "0.12"\n',
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/sdk/Cargo.toml", report)
        self.assertIn("only nervix-primitives depends on `parking_lot`", report)


class ManifestRenameTests(CheckTestCase):
    """A manifest that renames a governed crate hides its paths from the source rules."""

    def test_a_renamed_governed_crate_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + 'runtime = { package = "tokio", version = "1" }\n'
                + 'prims = { package = "nervix-primitives", path = "../primitives" }\n'
                + '\n[dev-dependencies]\nhelpers = { package = "futures-util", version = "0.3" }\n'
            },
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "the dependencies entry `runtime` renames `tokio`, which hides its governed paths",
            report,
        )
        self.assertIn("the dependencies entry `prims` renames `nervix-primitives`", report)
        self.assertIn("the dev-dependencies entry `helpers` renames `futures-util`", report)

    def test_a_workspace_rename_used_by_a_member_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "Cargo.toml": WORKSPACE + 'tk = { package = "tokio", version = "1" }\n',
                "crates/vocabulary/Cargo.toml": VOCABULARY + "tk = { workspace = true }\n",
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("the dependencies entry `tk` renames `tokio`", report)

    def test_a_hyphenated_name_and_an_ungoverned_rename_pass(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/vocabulary/Cargo.toml": VOCABULARY
                + 'tokio_util = { package = "tokio-util", version = "0.7" }\n'
                + 'hashing = { package = "ahash", version = "0.8" }\n'
            },
        )
        self.assertEqual(status, 0, report)

    def test_a_new_package_outside_the_member_list_is_checked(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "crates/fresh/Cargo.toml": '[package]\nname = "nervix-fresh"\n\n[dependencies]\n'
                'dashmap = "6"\n',
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/fresh/Cargo.toml", report)
        self.assertIn("only nervix-primitives depends on `dashmap`", report)


class LoomModuleFileTests(CheckTestCase):
    """A module declared out of line and compiled only for Loom is model code, file and all."""

    def test_a_loom_only_module_file_is_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": '#[cfg(all(test, feature = "loom"))]\nmod loom_models;\n',
                "crates/engine/src/loom_models.rs": (
                    "use nervix_primitives::sync::Notify;\n"
                    "fn model() { let _gate = nervix_primitives::sync::blocking::Mutex::new(0); }\n"
                ),
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/engine/src/loom_models.rs:1: primitive boundary: Loom model code names "
            "`nervix_primitives::sync::Notify`",
            report,
        )
        self.assertIn("Loom model code names `nervix_primitives::sync::blocking::Mutex::new`", report)

    def test_a_module_a_loom_build_excludes_is_not_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]\n'
                    '#[path = "lib_tests.rs"]\nmod tests;\n'
                ),
                "crates/engine/src/lib_tests.rs": (
                    "use nervix_primitives::sync::watch;\n"
                    "fn wait() { nervix_primitives::time::sleep(DELAY); }\n"
                ),
            }
        )
        self.assertEqual(status, 0, report)

    def test_a_module_an_ordinary_build_also_compiles_is_not_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(any(test, feature = "loom"))]\n'
                    "mod shared { fn wait() { nervix_primitives::time::sleep(DELAY); } }\n"
                ),
            }
        )
        self.assertEqual(status, 0, report)

    def test_a_nested_condition_that_still_requires_loom_is_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(not(not(feature = "loom")))]\nmod doubled;\n'
                    '#[cfg(all(test, any(feature = "loom", all(feature = "loom", test)),))]\n'
                    "mod grouped { fn wait() { nervix_primitives::time::sleep(DELAY); } }\n"
                ),
                "crates/engine/src/doubled.rs": "use nervix_primitives::sync::Notify;\n",
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/engine/src/doubled.rs:1: primitive boundary: Loom model code names "
            "`nervix_primitives::sync::Notify`",
            report,
        )
        self.assertIn("Loom model code names `nervix_primitives::time::sleep`", report)

    def test_a_module_whose_condition_cannot_be_read_is_rejected(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(all(test, feature = loom))]\nmod declared;\n'
                    '#[cfg(not(test, feature = "loom"))]\nmod inline {}\n'
                    '#[cfg(test & feature = "loom")]\nmod joined {}\n'
                ),
                "crates/engine/src/declared.rs": "fn ordinary() {}\n",
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/engine/src/lib.rs:5: primitive boundary: cannot read "
            '`cfg(test & feature = "loom")`',
            report,
        )
        self.assertIn(
            "crates/engine/src/lib.rs:1: primitive boundary: cannot read "
            "`cfg(all(test, feature = loom))`",
            report,
        )
        self.assertIn(
            "crates/engine/src/lib.rs:3: primitive boundary: cannot read "
            '`cfg(not(test, feature = "loom"))`',
            report,
        )

    def test_a_path_attribute_and_a_nested_declaration_are_followed(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/cancellation.rs": (
                    '#[cfg(feature = "loom")]\n#[path = "models/cancel.rs"]\nmod models;\n'
                ),
                "crates/engine/src/models/cancel.rs": "mod helpers;\n",
                "crates/engine/src/models/cancel/helpers.rs": (
                    "fn wait() { nervix_primitives::time::sleep(DELAY); }\n"
                ),
            }
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/engine/src/models/cancel/helpers.rs:1: primitive boundary: Loom model code "
            "names `nervix_primitives::time::sleep`",
            report,
        )

    def test_shared_ownership_and_permitted_unmodeled_primitives_pass_in_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": '#[cfg(feature = "loom")]\nmod runner;\n',
                "crates/engine/src/runner.rs": RUNNER
                + "use nervix_primitives::sync::{Arc, atomic::AtomicBool};\n"
                + "fn model() { let _flag = Arc::new(AtomicBool::new(false)); }\n",
            }
        )
        self.assertEqual(status, 0, report)

    def test_a_module_declared_without_loom_is_not_model_code(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": '#[cfg(not(feature = "loom"))]\nmod ordinary;\n',
                "crates/engine/src/ordinary.rs": "use nervix_primitives::sync::Notify;\n",
            }
        )
        self.assertEqual(status, 0, report)


class ModeCfgTests(CheckTestCase):
    """An execution mode is a feature of the boundary, never a global cfg."""

    def test_a_bare_mode_cfg_in_source_fails(self) -> None:
        report = self.assert_rejected(
            "#[cfg(loom)]\nmod models {}\n"
            "fn f() -> bool { cfg!(any(test, shuttle)) }\n"
            '#[cfg_attr(not(turmoil), path = "real.rs")]\nmod network;\n',
            "crates/engine/src/lib.rs:1",
            "`cfg(loom)` selects an execution mode through a global cfg",
            '`feature = "loom"`',
            "`cfg(shuttle)` selects an execution mode",
            "`cfg(turmoil)` selects an execution mode",
        )
        self.assertIn("crates/engine/src/lib.rs:3", report)

    def test_a_bare_diagnostic_mode_cfg_fails(self) -> None:
        self.assert_rejected(
            "#[cfg(deloxide)]\nmod diagnostics {}\n",
            "crates/engine/src/lib.rs:1",
            "`cfg(deloxide)` selects an execution mode through a global cfg",
            '`feature = "deloxide"`',
        )

    def test_a_global_diagnostic_mode_cfg_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={"justfile": 'test-deloxide:\n    RUSTFLAGS="--cfg deloxide" cargo test\n'},
        )
        self.assertEqual(status, 1)
        self.assertIn("justfile:2: primitive boundary: `--cfg deloxide` selects an execution mode", report)

    def test_a_mode_feature_and_tokios_unstable_cfg_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    '#[cfg(feature = "loom")]\nmod models {}\n'
                    "fn unstable() -> bool { cfg!(tokio_unstable) }\n"
                    "fn shuttle() {}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_a_global_mode_cfg_fails_even_in_a_turmoil_recipe(self) -> None:
        status, report = self.check(
            {"crates/engine/build.rs": 'fn main() { println!("cargo::rustc-cfg=loom"); }\n'},
            manifests={
                "justfile": (
                    'test-turmoil budget="480":\n'
                    '    RUSTFLAGS="--cfg turmoil --cfg tokio_unstable" cargo test\n'
                ),
                ".cargo/config.toml": '[build]\nrustflags = ["--cfg", "shuttle"]\n',
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("justfile:2: primitive boundary: `--cfg turmoil` selects an execution mode", report)
        self.assertNotIn("`--cfg tokio_unstable` changes", report)
        self.assertIn(".cargo/config.toml:2: primitive boundary: `--cfg shuttle`", report)
        self.assertIn("crates/engine/build.rs: primitive boundary: `--cfg loom`", report)


class AnalysisCfgTests(CheckTestCase):
    """The analysis cfg gates lint annotations, never selects code, and only the driver sets it."""

    def test_contracts_and_expectations_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "#![cfg_attr(\n"
                    "    nervix_lint,\n"
                    '    nervix::context(lifecycle, reason = "installs, once per domain")\n'
                    ")]\n"
                    '#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "each batch"))]\n'
                    "fn process() {}\n"
                    "#[cfg_attr(\n"
                    "    nervix_lint,\n"
                    '    nervix::dispatch(reason = "the callback (bounded), called once"),\n'
                    '    expect(nervix::sync_acquisition, reason = "retained debt"),\n'
                    ")]\n"
                    "fn dispatch() {}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)

    def test_the_cfg_selecting_code_fails(self) -> None:
        report = self.assert_rejected(
            "#[cfg(nervix_lint)]\nfn analyzed_only() {}\n"
            "#[cfg(not(nervix_lint))]\nfn shipped_only() {}\n"
            "fn f() -> bool { cfg!(all(test, nervix_lint)) }\n",
            "crates/engine/src/lib.rs:1: primitive boundary: `cfg(nervix_lint)` selects code for "
            "the analysis build alone",
            "the analysis cfg only gates `nervix::` contracts and lint expectations through "
            "`cfg_attr`",
        )
        self.assertIn("crates/engine/src/lib.rs:3", report)
        self.assertIn("crates/engine/src/lib.rs:5", report)

    def test_an_attribute_that_changes_the_compiled_code_fails(self) -> None:
        report = self.assert_rejected(
            '#[cfg_attr(nervix_lint, path = "analyzed.rs")]\nmod selected;\n'
            "#[cfg_attr(not(nervix_lint), derive(Debug))]\nstruct Shipped;\n"
            '#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "x"), cfg(any()))]\n'
            "fn hidden() {}\n",
            "crates/engine/src/lib.rs:1: primitive boundary: `cfg_attr(nervix_lint, path)` changes "
            "what the analysis build compiles",
            "`cfg_attr(nervix_lint, derive)`",
            "`cfg_attr(nervix_lint, cfg)`",
        )
        self.assertIn("crates/engine/src/lib.rs:3", report)
        self.assertIn("crates/engine/src/lib.rs:5", report)

    def test_a_bypass_behind_an_annotation_is_still_rejected(self) -> None:
        self.assert_rejected(
            '#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "installs"))]\n'
            "fn install() { let _lock = std::sync::Mutex::new(0); }\n",
            "crates/engine/src/lib.rs:2: primitive boundary: `std::sync::Mutex::new` bypasses the "
            "boundary",
        )

    def test_a_build_that_sets_the_cfg_fails(self) -> None:
        status, report = self.check(
            {"crates/engine/build.rs": 'fn main() { println!("cargo::rustc-cfg=nervix_lint"); }\n'},
            manifests={
                "justfile": 'lint:\n    RUSTFLAGS="--cfg nervix_lint" cargo clippy\n',
                ".cargo/config.toml": '[build]\nrustflags = ["--cfg", "nervix_lint"]\n',
                ".github/workflows/check.yaml": (
                    "jobs:\n  checks:\n    env:\n      RUSTFLAGS: --cfg=nervix_lint\n"
                ),
            },
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "justfile:2: primitive boundary: `--cfg nervix_lint` would enable the analysis "
            "annotations in every crate of a build; only the synchronization analysis driver sets "
            "it",
            report,
        )
        self.assertIn(".cargo/config.toml:2: primitive boundary: `--cfg nervix_lint`", report)
        self.assertIn(
            ".github/workflows/check.yaml:4: primitive boundary: `--cfg nervix_lint`", report
        )
        self.assertIn("crates/engine/build.rs: primitive boundary: `--cfg nervix_lint`", report)

    def test_the_driver_passing_the_cfg_to_the_compiler_it_wraps_passes(self) -> None:
        status, report = self.check(
            {
                "tools/lint/driver/src/main.rs": (
                    "fn analysis_arguments() -> [&'static str; 2] {\n"
                    '    ["--cfg=nervix_lint", "--check-cfg=cfg(nervix_lint)"]\n'
                    "}\n"
                )
            }
        )
        self.assertEqual(status, 0, report)


class AuthoredSourceSurfaceTests(CheckTestCase):
    """An isolated tooling workspace is authored source; what a build wrote is not."""

    CACHE_TAG = (
        "Signature: 8a477f597d28d172789f06886806bc55\n"
        "# This file is a cache directory tag created by cargo.\n"
    )
    GENERATED = {
        "tools/lint/fixture-macros/target/debug/build/macros-1/out/generated.rs": (
            "use std::sync::Mutex;\n"
        ),
    }
    PACKAGED = {
        "tools/lint/fixture-macros/target/package/macros-0.1.0/Cargo.toml": (
            '[package]\nname = "packaged-macros"\n\n[dependencies]\ntriomphe = "0.1"\n'
        ),
    }

    def test_an_isolated_tooling_workspace_is_held_to_the_boundary(self) -> None:
        status, report = self.check(
            {
                "tools/lint/driver/src/main.rs": "use triomphe::Arc;\n",
                "tools/lint/fixture-macros/src/lib.rs": "use std::sync::atomic::AtomicBool;\n",
                "tools/lint/fixtures/tests/semantics/case.rs": "use std::sync::Arc;\n",
            },
            manifests={
                "tools/lint/Cargo.toml": '[workspace]\nmembers = ["driver"]\nresolver = "3"\n',
                "tools/lint/driver/Cargo.toml": (
                    '[package]\nname = "lint-driver"\n\n[dependencies]\ntriomphe = "0.1"\n'
                ),
            },
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "tools/lint/driver/src/main.rs:1: primitive boundary: `triomphe::Arc`", report
        )
        self.assertIn("tools/lint/fixture-macros/src/lib.rs:1: primitive boundary:", report)
        self.assertIn(
            "tools/lint/fixtures/tests/semantics/case.rs:1: primitive boundary: `std::sync::Arc`",
            report,
        )
        self.assertIn(
            "tools/lint/driver/Cargo.toml: primitive boundary: only nervix-primitives depends on "
            "`triomphe`",
            report,
        )

    def test_a_nested_cargo_build_directory_is_not_authored_source(self) -> None:
        status, report = self.check(
            {
                "tools/lint/fixture-macros/target/CACHEDIR.TAG": self.CACHE_TAG,
                **self.GENERATED,
            },
            manifests=self.PACKAGED,
        )
        self.assertEqual(status, 0, report)

    def test_a_directory_cargo_did_not_tag_is_authored_source(self) -> None:
        status, report = self.check(
            {
                "tools/lint/fixture-macros/target/CACHEDIR.TAG": "an authored note\n",
                **self.GENERATED,
            },
            manifests=self.PACKAGED,
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "tools/lint/fixture-macros/target/debug/build/macros-1/out/generated.rs:1: primitive "
            "boundary: `std::sync::Mutex`",
            report,
        )
        self.assertIn(
            "tools/lint/fixture-macros/target/package/macros-0.1.0/Cargo.toml: primitive boundary: "
            "only nervix-primitives depends on `triomphe`",
            report,
        )

    def test_a_tag_at_the_repository_root_excludes_nothing(self) -> None:
        status, report = self.check(
            {"CACHEDIR.TAG": self.CACHE_TAG, "crates/engine/src/lib.rs": "use triomphe::Arc;\n"}
        )
        self.assertEqual(status, 1)
        self.assertIn("crates/engine/src/lib.rs:1: primitive boundary: `triomphe::Arc`", report)


class PermissionScopeTests(CheckTestCase):
    """A permission covers one governed Rust file and names real items of the unmodeled path."""

    def test_a_directory_or_glob_permission_fails(self) -> None:
        for path in ("crates/engine/src/", "crates/engine/src/*.rs", "../outside.rs"):
            with self.subTest(path=path):
                status, report = self.check(
                    {},
                    permissions=PERMISSION.replace("crates/engine/src/runner.rs", path),
                )
                self.assertEqual(status, 1)
                self.assertIn("a permission covers one Rust file", report)

    def test_an_unknown_item_fails(self) -> None:
        status, report = self.check(
            {},
            permissions=PERMISSION.replace(
                '"sync::atomic::Ordering"', '"sync::atomic::Ordering", "sync::Everything"'
            ),
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "lists `sync::Everything`, which is not an item of `nervix_primitives::unmodeled`",
            report,
        )

    def test_a_permission_for_a_file_the_boundary_does_not_govern_is_stale(self) -> None:
        status, report = self.check(
            {},
            permissions=PERMISSION
            + PERMISSION.replace("crates/engine/src/runner.rs", "crates/primitives/src/sync.rs"),
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "stale permission: crates/primitives/src/sync.rs is not a Rust file the boundary governs",
            report,
        )


class ReleaseBinaryTests(CheckTestCase):
    """Every binary the release image builds refuses a build that selects an execution mode."""

    DOCKERFILE = (
        "FROM rust AS builder\n"
        "RUN cargo build --release --package nervix-engine \\\n"
        "    && cargo auditable build --release --target x86_64 --package nervix-engine\n"
        "RUN cargo test --package nervix-vocabulary\n"
    )

    def test_a_released_binary_that_declares_the_guard_passes(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/main.rs": (
                    'nervix_primitives::product_binary!("nervix-engine");\nfn main() {}\n'
                ),
                "crates/engine/src/bin/probe.rs": (
                    'nervix_primitives::product_binary!("probe");\nfn main() {}\n'
                ),
            },
            manifests={"Dockerfile.debian": self.DOCKERFILE},
        )
        self.assertEqual(status, 0, report)

    def test_a_released_binary_with_a_diagnostic_form_declares_the_guard(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/main.rs": (
                    'nervix_primitives::product_binary!("nervix-engine", diagnostic);\nfn main() {}\n'
                ),
                "crates/engine/src/bin/probe.rs": (
                    'nervix_primitives::product_binary!("probe");\nfn main() {}\n'
                ),
            },
            manifests={"Dockerfile.debian": self.DOCKERFILE},
        )
        self.assertEqual(status, 0, report)

    def test_a_released_binary_without_the_guard_fails(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/main.rs": (
                    'nervix_primitives::product_binary!("nervix-engine");\nfn main() {}\n'
                ),
                "crates/engine/src/bin/probe.rs": "fn main() {}\n",
            },
            manifests={"Dockerfile.debian": self.DOCKERFILE},
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "crates/engine/src/bin/probe.rs: primitive boundary: the release image ships `probe`, "
            'so its crate root declares `nervix_primitives::product_binary!("probe")`',
            report,
        )
        self.assertNotIn("crates/engine/src/main.rs", report)

    def test_a_declared_binary_names_itself(self) -> None:
        status, report = self.check(
            {"crates/engine/src/serve.rs": 'nervix_primitives::product_binary!("other");\n'},
            manifests={
                "Dockerfile.debian": self.DOCKERFILE,
                "crates/engine/Cargo.toml": ENGINE
                + '\n[[bin]]\nname = "serve"\npath = "src/serve.rs"\n',
            },
        )
        self.assertEqual(status, 1)
        self.assertIn("the release image ships `serve`", report)

    def test_an_unknown_released_package_fails(self) -> None:
        status, report = self.check(
            {},
            manifests={
                "Dockerfile.debian": "RUN cargo build --release --package nervix-missing\n"
            },
        )
        self.assertEqual(status, 1)
        self.assertIn(
            "Dockerfile.debian:1: primitive boundary: the release build names package "
            "`nervix-missing`, which no manifest declares",
            report,
        )


if __name__ == "__main__":
    unittest.main()
