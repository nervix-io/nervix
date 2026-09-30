//! Tooling fixture for compiler diagnostics across cache namespaces and worktrees.

#[test]
fn compiler_diagnostic_paths() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/collection_type.rs");
}
