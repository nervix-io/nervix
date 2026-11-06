//! Repository assertion harness, outside the product layer order.
//! Owns: mode-specific fixture evidence and error-replacement assertions.
//! Depends on: prepared current reports and diagnostics.
//! Must not know: compiler execution or runtime model exploration.

use std::{fs, path::PathBuf};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_ratchet::evidence::{Ending, MatrixReport};

#[test]
fn selected_backends_keep_authored_acquisitions_and_replacement_outcomes() {
    let target = PathBuf::from(
        std::env::var_os("NERVIX_RATCHET_TARGET_DIR")
            .assured("the recipe supplies the evidence root"),
    );
    for (mode, expected) in [
        ("shuttle", 24),
        ("loom", 25),
        ("turmoil", 25),
        ("deloxide", 25),
    ] {
        let report: MatrixReport =
            nervix_ratchet::io::read(&target.join(format!("typed-ratchet/fixture-{mode}.json")))
                .assured("the recipe collected this mode's complete report");
        assert!(report.complete);
        assert_eq!(report.findings.len(), expected, "{mode}");
        let findings: Vec<_> = report
            .findings
            .iter()
            .flat_map(|site| site.configurations.values())
            .flatten()
            .collect();
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.owner.contains("::borrowed_iteration"))
                .count(),
            1
        );
        if mode == "deloxide" {
            assert!(findings.iter().any(|finding| {
                finding
                    .receiver
                    .starts_with("nervix_primitives::sync::blocking::tracked")
            }));
            assert!(
                !findings
                    .iter()
                    .any(|finding| finding.receiver.starts_with("lock_api"))
            );
        }
        if mode == "shuttle" {
            assert!(
                findings
                    .iter()
                    .any(|finding| finding.receiver.starts_with("shuttle_dashmap_impl"))
            );
            assert!(findings.iter().any(|finding| {
                finding
                    .receiver
                    .starts_with("nervix_primitives::collections::scheduled")
            }));
        }
        let base = target.join("typed-ratchet/contracts");
        let allowed: Ending = nervix_ratchet::io::read(
            &base.join(format!("error_replacement_allowed-{mode}/ending.json")),
        )
        .assured("the allowed mode fixture was prepared");
        assert!(allowed.succeeded());
        let refused: Ending =
            nervix_ratchet::io::read(&base.join(format!("error_replacement-{mode}/ending.json")))
                .assured("the refused mode fixture was prepared");
        assert!(!refused.succeeded());
        let stderr = fs::read_to_string(base.join(format!("error_replacement-{mode}/stderr")))
            .assured("the compiler diagnostics were retained");
        let mut count = 0;
        for line in stderr.lines() {
            if line.starts_with('{') {
                let value: serde_json::Value =
                    serde_json::from_str(line).assured("a rustc diagnostic is valid JSON");
                if value["code"]["code"] == "nervix::replaced_error" {
                    count += 1;
                }
            }
        }
        assert_eq!(count, 16, "{mode}: {stderr}");
    }
}
