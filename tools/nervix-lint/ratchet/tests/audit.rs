//! Repository process-test harness, outside the product layer order.
//! Owns: the audit command's public arguments and failure reporting.
//! Depends on: the built production binary and isolated Git/process fixtures.
//! Must not know: compiler implementation or product graph execution.

use std::process::Command;

use meticulous::ResultExt as _;

#[test]
fn audit_help_exposes_the_complete_provenance_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_nervix-ratchet"))
        .args(["audit", "--help"])
        .output()
        .assured("the built audit command can print its help");
    assert!(
        output.status.success(),
        "audit help failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let help = String::from_utf8(output.stdout).assured("command help is UTF-8");
    for argument in [
        "--root",
        "--target-dir",
        "--inventory",
        "--configuration",
        "--pull-requests",
        "--output-dir",
    ] {
        assert!(
            help.contains(argument),
            "audit help omitted {argument}: {help}"
        );
    }
}
