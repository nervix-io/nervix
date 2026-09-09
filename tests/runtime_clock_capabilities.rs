//! Compile-time boundaries between logical and physical runtime deadlines.

#[test]
fn logical_and_physical_deadlines_cannot_be_interchanged() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/runtime_clock_capabilities/*.rs");
}
