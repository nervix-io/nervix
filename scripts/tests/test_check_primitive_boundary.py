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


def write_repository(
    root: Path,
    sources: dict[str, str],
    manifests: dict[str, str] | None = None,
    permissions: str = PERMISSION,
) -> None:
    files = {
        "Cargo.toml": WORKSPACE,
        "crates/primitives/Cargo.toml": PRIMITIVES,
        "crates/primitives/src/sync.rs": "pub use std::sync::atomic::AtomicBool;\n",
        "crates/primitives/unmodeled-permissions.toml": permissions,
        "crates/harness/Cargo.toml": HARNESS,
        "crates/engine/Cargo.toml": ENGINE,
        "crates/engine/src/runner.rs": RUNNER,
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
    ) -> tuple[int, str]:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        write_repository(root, sources, manifests, permissions)
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


class FamilyTests(CheckTestCase):
    """The families beyond atomics: async and thread-blocking synchronization, tasks, the runtime
    and its macros, streams, publication, concurrent collections, threads and model backends."""

    def test_approved_family_paths_pass(self) -> None:
        status, report = self.check(
            {
                "crates/engine/src/lib.rs": (
                    "use std::sync::Arc;\n"
                    "use nervix_primitives::{sync::{Notify, blocking::Mutex, watch}, task};\n"
                    "#[cfg(feature = \"shuttle\")]\nextern crate shuttle_tokio as tokio;\n"
                    "nervix_primitives::thread_local! { static SEEN: u8 = const { 0 }; }\n"
                    "#[nervix_primitives::test]\n"
                    "async fn waits() {\n"
                    "    task::consume_budget().await;\n"
                    "    nervix_primitives::select! { () = tokio::time::sleep(DURATION) => {} }\n"
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
            "use tokio::{sync::watch, time::sleep};\n",
            "`tokio::sync::watch` bypasses the boundary",
        )
        self.assertNotIn("tokio::time", report)

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

    def test_an_imported_sync_module_fails_where_it_reaches_blocking_synchronization(self) -> None:
        report = self.assert_rejected(
            "use std::sync;\nfn f() -> sync::Mutex<u8> { todo() }\nfn g() -> sync::Arc<u8> { todo() }\n",
            "`sync::Mutex` reaches thread-blocking synchronization through an imported `sync` module",
        )
        self.assertNotIn("sync::Arc", report)

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

    def test_a_backend_alias_fails_and_the_timer_alias_passes(self) -> None:
        report = self.assert_rejected(
            '#[cfg(feature = "shuttle")]\nextern crate shuttle_parking_lot as parking_lot;\n'
            '#[cfg(feature = "shuttle")]\nextern crate shuttle_tokio as tokio;\n'
            "extern crate tokio as tokio_real;\n",
            "`extern crate shuttle_parking_lot as parking_lot` selects a backend outside the boundary",
            "`extern crate tokio as tokio_real` selects a backend outside the boundary",
        )
        self.assertNotIn("shuttle_tokio as tokio`", report)

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
            }
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


if __name__ == "__main__":
    unittest.main()
