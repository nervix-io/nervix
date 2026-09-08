//! Compile-time boundaries for authoritative runtime-state operations.

#[test]
fn forbidden_runtime_state_capability_calls_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/runtime_state_capabilities/*.rs");
}
