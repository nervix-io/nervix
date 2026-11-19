//! Repository tooling, outside the product layer order.
//! Owns: the identity and evidence of counted branch-ownership declarations.
//! Depends on: the verified compiler matrix and the branch ownership baseline entries.
//! Must not know: collection syntax, runtime code or any permitted collection.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    path::Path,
    process::Command,
};

use anyhow::{Context as _, Result, bail, ensure};

use super::Site;
use crate::evidence::MatrixReport;

pub(super) fn collect(report: &MatrixReport) -> Result<BTreeMap<String, Site>> {
    ensure!(
        report.complete && report.matrix_complete && report.diagnostics_checked,
        "branch ownership requires a complete, checked compiler matrix"
    );
    let mut sites = BTreeMap::new();
    for configuration in &report.evidence {
        for compilation in configuration.reports.values() {
            for violation in &compilation.branch_inversions {
                let declaration = format!("{}::{}", violation.owner, violation.declaration);
                let reason = violation
                    .expectation_reason
                    .as_deref()
                    .filter(|reason| nervix_lint_report::branch_repair_task(reason).is_some())
                    .with_context(|| {
                        format!(
                            "{}:{}: branch collection {declaration} requires one expectation that \
                             names its ClickUp repair task",
                            violation.span.site.path, violation.span.line
                        )
                    })?;
                let site = Site {
                    path: violation.span.site.path.clone(),
                    line: usize::try_from(violation.span.line)?,
                    detail: format!("{declaration} ({reason})"),
                };
                match sites.entry(declaration) {
                    Entry::Vacant(entry) => {
                        entry.insert((violation.span.site.clone(), site));
                    }
                    Entry::Occupied(entry) => {
                        ensure!(
                            entry.get() == &(violation.span.site.clone(), site.clone()),
                            "branch declarations share the same identity: {} and {}",
                            entry.get().1.render(),
                            site.render()
                        );
                    }
                }
            }
        }
    }
    Ok(sites
        .into_iter()
        .map(|(name, (_, site))| (name, site))
        .collect())
}

pub(super) fn verify(sites: &BTreeMap<String, Site>, pinned: &BTreeSet<String>) -> Result<()> {
    let added: Vec<_> = sites
        .iter()
        .filter(|(identity, _)| !pinned.contains(*identity))
        .map(|(_, site)| site.render())
        .collect();
    if !added.is_empty() {
        bail!(
            "new branch ownership expectations are forbidden:\n{}",
            added.join("\n")
        );
    }
    Ok(())
}

pub(super) fn verify_previous_baseline(root: &Path, current: &BTreeSet<String>) -> Result<()> {
    let merge_ref = std::env::var("GITHUB_REF")
        .is_ok_and(|value| value.starts_with("refs/pull/") && value.ends_with("/merge"));
    let revision = if merge_ref { "HEAD^1" } else { "origin/main" };
    let git = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["show", &format!("{revision}:debt-baseline.json")])
        .output()
        .context("cannot read the accepted debt baseline")?;
    if !git.status.success() && !merge_ref {
        return Ok(());
    }
    ensure!(
        git.status.success(),
        "cannot read the pull request's base debt baseline: {}",
        String::from_utf8_lossy(&git.stderr)
    );
    let previous: serde_json::Value = serde_json::from_slice(&git.stdout)?;
    // The introducing pull request is the only one whose accepted base has no branch inventory.
    if let Some(declarations) = previous.get("branch_inversion_sites") {
        let accepted = serde_json::from_value(declarations.clone())?;
        let added: Vec<_> = current.difference(&accepted).cloned().collect();
        ensure!(
            added.is_empty(),
            "the debt baseline cannot add branch ownership declarations:\n{}",
            added.join("\n")
        );
    }
    Ok(())
}
