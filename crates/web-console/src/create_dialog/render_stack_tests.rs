//! The stack an unoptimized render of the create forms needs, held below the default's edge.
//!
//! Layer: edges.
//!
//! - **Owns.** Running the create dialog's render tests again with three quarters of the stack a
//!   test thread has by default.
//! - **Depends on.** This test executable and the render tests it holds.
//! - **Must not know.** What any form renders.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::process::Command;

use meticulous::ResultExt as _;

/// Three quarters of the 2 MiB a test thread has unless `RUST_MIN_STACK` sets another size.
const THREE_QUARTERS_OF_THE_DEFAULT_TEST_STACK_BYTES: usize = 1536 * 1024;

/// The test modules that render the create dialog, as test name filters.
const RENDER_TEST_MODULES: [&str; 3] = [
    "create_dialog::emitter_form_tests::",
    "create_dialog::tests::",
    "create_dialog::visual_forms_tests::",
];

/// A thread that overflows its stack aborts the whole test process, so the render tests run again
/// in a child process of this executable whose test threads have three quarters of the default
/// stack. A form whose render outgrows that fails here while the default still holds it, instead
/// of first failing at the default's edge.
#[test]
fn every_create_form_renders_on_three_quarters_of_the_default_test_stack() {
    let executable = std::env::current_exe().assured("a running test executable has a path");
    let output = Command::new(executable)
        .args(RENDER_TEST_MODULES)
        .env(
            "RUST_MIN_STACK",
            THREE_QUARTERS_OF_THE_DEFAULT_TEST_STACK_BYTES.to_string(),
        )
        .output()
        .assured("the test executable starts again as a child process");
    let report = String::from_utf8_lossy(&output.stdout);
    let errors = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "the create dialog's render tests fail with \
         {THREE_QUARTERS_OF_THE_DEFAULT_TEST_STACK_BYTES} bytes of stack per test thread:\n\
         {report}{errors}"
    );
    for module in RENDER_TEST_MODULES {
        assert!(
            report.contains(&format!("test {module}")),
            "no test of {module} ran in the child process:\n{report}"
        );
    }
}
