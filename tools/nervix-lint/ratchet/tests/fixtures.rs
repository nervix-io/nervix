//! Repository assertion harness, outside the product layer order.
//! Owns: ordinary fixture target coverage and acquisition-owner assertions.
//! Depends on: a complete production matrix report prepared by the recipe.
//! Must not know: compiler execution or host state.

use std::{collections::BTreeMap, path::PathBuf};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_ratchet::evidence::MatrixReport;

#[test]
fn the_compiler_distinguishes_real_acquisitions_and_records_generated_exclusions() {
    let target = std::env::var_os("NERVIX_RATCHET_TARGET_DIR")
        .assured("the recipe supplies the evidence root");
    let report: MatrixReport = nervix_ratchet::io::read(
        &PathBuf::from(target).join("typed-ratchet/fixture-ordinary.json"),
    )
    .assured("run the compiler preparation recipe before its assertion-only tests");
    assert!(report.complete);
    let mut owners = BTreeMap::<String, usize>::new();
    let mut expansions = 0;
    for site in &report.findings {
        for findings in site.configurations.values() {
            for finding in findings {
                let owner = finding
                    .owner
                    .rsplit("::")
                    .next()
                    .assured("splitting an owner always has a final segment");
                *owners.entry(owner.into()).or_default() += 1;
                if !finding.expansion.is_empty() {
                    expansions += 1;
                }
            }
        }
    }
    for (owner, count) in [
        ("locked_collection", 1),
        ("owned_collections", 0),
        ("io_operations", 0),
        ("custom_methods", 0),
        ("shared_map_operations", 11),
        ("aliases_ufcs_and_deref", 2),
        ("access", 1),
        ("read_write_lock", 4),
        ("borrowed_iteration", 1),
        ("reserve_shared_map", 1),
        ("owned_iteration", 0),
    ] {
        assert_eq!(owners.get(owner).copied().unwrap_or(0), count, "{owner}");
    }
    assert!(expansions > 0);
    let excluded: u64 = report
        .evidence
        .iter()
        .flat_map(|entry| entry.reports.values())
        .map(|report| u64::from(report.excluded_generated))
        .sum();
    assert!(excluded > 0);
}
